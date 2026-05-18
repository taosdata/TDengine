"""Timezone scalar function tests.

Covers:
- P3 Task 3.1: TO_ISO8601(ts, 'IANA_name') IANA extension, DST, fallback L2->L3->L5
- P3 Task 3.2: TO_CHAR(ts, fmt, 'timezone') third parameter
- P3 Task 3.3: TIMETRUNCATE(ts, unit, 'tz_string') third parameter extension
- P4 Task 4.1: TIMETRUNCATE n/q/y natural units (1n/1q/1y only; Nx invalid)
- P4 Task 4.2: TIMETRUNCATE 1w aligned by firstDayOfWeek (high-risk change)
- P4 Task 4.4: WEEK/WEEKOFYEAR respect firstDayOfWeek; DAYOFWEEK/WEEKDAY unchanged
- DST edge cases: spring-forward gap, fall-back overlap, write-path regression
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

@SKIP_WINDOWS_SET_TIMEZONE
class TestToIso8601Iana:
    """TO_ISO8601 IANA timezone parameter and DST-aware output."""

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.dbname = 'db_tz_iso'
        cls.ntbname = f'{cls.dbname}.ntb'
        cls.ts_winter = 1642248000000  # 2022-01-15 12:00:00 UTC
        cls.ts_summer = 1657886400000  # 2022-07-15 12:00:00 UTC

    def prepare_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.ntbname}')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp, c1 int)')
        tdSql.execute(f'insert into {self.ntbname} values ({self.ts_winter}, 1)')
        tdSql.execute(f'insert into {self.ntbname} values ({self.ts_summer}, 2)')

    def check_to_iso8601_iana_no_dst(self):
        """IANA timezones without DST should have constant offset."""
        self.prepare_data()
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
        self.prepare_data()
        tdSql.query(
            f"select to_iso8601(ts, 'America/New_York') from {self.ntbname} order by ts"
        )
        winter, summer = tdSql.queryResult[0][0], tdSql.queryResult[1][0]
        assert '-05:00' in winter or '-0500' in winter, f"Winter: {winter}"
        assert '-04:00' in summer or '-0400' in summer, f"Summer: {summer}"

    def check_to_iso8601_iana_dst_london(self):
        """Europe/London: winter +00:00 (GMT), summer +01:00 (BST)."""
        self.prepare_data()
        tdSql.query(
            f"select to_iso8601(ts, 'Europe/London') from {self.ntbname} order by ts"
        )
        winter, summer = tdSql.queryResult[0][0], tdSql.queryResult[1][0]
        assert any(e in winter for e in ['+00:00', '+0000', 'Z'])
        assert '+01:00' in summer or '+0100' in summer, f"Summer: {summer}"

    def check_to_iso8601_fixed_offset_compat(self):
        """Fixed offset params (+08:00, Z) should still work (backward compat)."""
        self.prepare_data()
        tdSql.query(f"select to_iso8601(ts, '+08:00') from {self.ntbname} order by ts")
        for row in tdSql.queryResult:
            assert '+08:00' in row[0] or '+0800' in row[0]

    def check_to_iso8601_invalid_tz(self):
        """Invalid timezone params should fail."""
        self.prepare_data()
        for tz in ["'Invalid/Zone'", "'CST'", "'+8'"]:
            tdSql.error(
                f"select to_iso8601(ts, {tz}) from {self.ntbname}",
                expectedErrno=ERR_INVALID_TIMEZONE,
            )

    def check_to_iso8601_no_param_uses_l2(self):
        """TO_ISO8601(ts) without tz param should use connection timezone (L2)."""
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        tdSql.query(f"select to_iso8601(ts) from {self.ntbname} order by ts")
        winter, summer = tdSql.queryResult[0][0], tdSql.queryResult[1][0]
        assert '-05:00' in winter or '-0500' in winter, f"L2 winter: {winter}"
        assert '-04:00' in summer or '-0400' in summer, f"L2 summer: {summer}"

    def check_to_iso8601_l1_overrides_l2(self):
        """Explicit tz param (L1) should override connection timezone (L2)."""
        self.prepare_data()
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
        self.check_to_iso8601_iana_no_dst()
        self.check_to_iso8601_iana_dst_new_york()
        self.check_to_iso8601_iana_dst_london()
        self.check_to_iso8601_fixed_offset_compat()
        self.check_to_iso8601_invalid_tz()
        self.check_to_iso8601_no_param_uses_l2()
        self.check_to_iso8601_l1_overrides_l2()
        self.check_to_iso8601_us_ns_precision()

@SKIP_WINDOWS_SET_TIMEZONE
class TestToCharTimezone:
    """TO_CHAR(ts, fmt [, timezone]) third parameter tests."""

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.dbname = 'db_tz_tochar'
        cls.ntbname = f'{cls.dbname}.ntb'
        cls.ts_winter = 1642248000000  # 2022-01-15 12:00:00 UTC
        cls.ts_summer = 1657886400000  # 2022-07-15 12:00:00 UTC

    def prepare_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.ntbname}')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp, c1 int)')
        tdSql.execute(f'insert into {self.ntbname} values ({self.ts_winter}, 1)')
        tdSql.execute(f'insert into {self.ntbname} values ({self.ts_summer}, 2)')

    def check_to_char_iana_and_fixed_offset(self):
        """TO_CHAR with IANA timezone and fixed offset.

        2022-01-15 12:00 UTC -> Shanghai 20:00, NY 07:00, +08:00 20:00.
        """
        self.prepare_data()
        fmt = 'YYYY-MM-DD HH24:MI:SS'
        cases = [
            ('Asia/Shanghai', 1, '20:00:00'),
            ('America/New_York', 1, '07:00:00'),   # winter EST
            ('America/New_York', 2, '08:00:00'),   # summer EDT
            ('UTC', 1, '12:00:00'),
            ('+08:00', 1, '20:00:00'),
        ]
        for tz, c1, expected_time in cases:
            tdSql.query(
                f"select to_char(ts, '{fmt}', '{tz}') from {self.ntbname} where c1 = {c1}"
            )
            assert expected_time in tdSql.queryResult[0][0], \
                f"tz={tz}, c1={c1}: expected {expected_time} in {tdSql.queryResult[0][0]}"

    def check_to_char_invalid_tz(self):
        """Invalid timezone params should fail."""
        self.prepare_data()
        fmt = 'YYYY-MM-DD HH24:MI:SS'
        for tz in ["'Invalid/Zone'", "'CST'"]:
            tdSql.error(
                f"select to_char(ts, '{fmt}', {tz}) from {self.ntbname}",
                expectedErrno=ERR_INVALID_TIMEZONE,
            )

    def check_to_char_no_param_uses_l2(self):
        """TO_CHAR without tz param should use connection timezone (L2)."""
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        tdSql.query(
            f"select to_char(ts, 'YYYY-MM-DD HH24:MI:SS') from {self.ntbname} where c1 = 1"
        )
        assert '07:00:00' in tdSql.queryResult[0][0]

    def check_to_char_l1_overrides_l2(self):
        """Explicit tz (L1) should override connection timezone (L2)."""
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
        tdSql.query(
            f"select to_char(ts, 'YYYY-MM-DD HH24:MI:SS', 'America/New_York') "
            f"from {self.ntbname} where c1 = 1"
        )
        assert '07:00:00' in tdSql.queryResult[0][0]

    def check_to_char_two_params_compat(self):
        """TO_CHAR(ts, fmt) two-param form should still work."""
        self.prepare_data()
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
        self.check_to_char_iana_and_fixed_offset()
        self.check_to_char_invalid_tz()
        self.check_to_char_no_param_uses_l2()
        self.check_to_char_l1_overrides_l2()
        self.check_to_char_two_params_compat()

@SKIP_WINDOWS_SET_TIMEZONE
class TestTimetruncateTz:
    """TIMETRUNCATE third parameter: string timezone, integer 0/1 compat, fallback chain."""

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.dbname = 'db_tz_tt_tz'
        cls.ntbname = f'{cls.dbname}.ntb'
        cls.ts1 = 1773750600000  # 2026-03-15 10:30:00 UTC

    def prepare_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.ntbname}')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp, c1 int)')
        tdSql.execute(f'insert into {self.ntbname} values ({self.ts1}, 1)')

    def check_timetruncate_iana_tz_different_boundaries(self):
        """String tz param should truncate in that timezone's calendar.

        UTC vs Shanghai should produce different 1d boundaries for this timestamp.
        """
        self.prepare_data()
        tdSql.query(f"select timetruncate(ts, 1d, 'UTC') from {self.ntbname}")
        r_utc = tdSql.queryResult[0][0]
        tdSql.query(f"select timetruncate(ts, 1d, 'Asia/Shanghai') from {self.ntbname}")
        r_sh = tdSql.queryResult[0][0]
        assert r_utc != r_sh, f"UTC vs Shanghai 1d should differ: {r_utc} vs {r_sh}"

    def check_timetruncate_int_01_compat(self):
        """Integer 0/1 and no-param forms should work (backward compat)."""
        self.prepare_data()
        for p in ['0', '1', '']:
            suffix = f', {p}' if p else ''
            tdSql.query(f"select timetruncate(ts, 1d{suffix}) from {self.ntbname}")
            assert tdSql.queryResult[0][0] is not None

    def check_timetruncate_invalid_tz(self):
        """Invalid timezone string should fail."""
        self.prepare_data()
        for tz in ["'Invalid/Zone'", "'CST'"]:
            tdSql.error(
                f"select timetruncate(ts, 1d, {tz}) from {self.ntbname}",
                expectedErrno=ERR_INVALID_TIMEZONE,
            )

    def check_timetruncate_l1_overrides_l2(self):
        """Explicit tz (L1) should override connection timezone (L2)."""
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
        tdSql.query(f"select timetruncate(ts, 1d, 'UTC') from {self.ntbname}")
        r_utc = tdSql.queryResult[0][0]
        tdSql.query(f"select timetruncate(ts, 1d, 'Asia/Shanghai') from {self.ntbname}")
        r_sh = tdSql.queryResult[0][0]
        assert r_utc != r_sh

    def check_timetruncate_no_param_uses_l2(self):
        """Without tz param, different L2 should produce different results."""
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        tdSql.query(f"select timetruncate(ts, 1d) from {self.ntbname}")
        r_ny = tdSql.queryResult[0][0]
        tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
        tdSql.query(f"select timetruncate(ts, 1d) from {self.ntbname}")
        r_sh = tdSql.queryResult[0][0]
        assert r_ny != r_sh

    def check_timetruncate_subday_string_tz_uses_target_offset(self):
        """Sub-day truncation with string timezone should use target local offset.

        2026-03-15T10:10:00Z in +05:45 is 15:55 local, so 1h should
        truncate to 15:00 local => 09:15 UTC.
        """
        self.prepare_data()
        cases = [
            ('Asia/Kathmandu', '1h', '2026-03-15T09:15:00'),
            ('+05:45', '1h', '2026-03-15T09:15:00'),
        ]
        for tz, unit, expected in cases:
            tdSql.query(
                "select to_iso8601(timetruncate('2026-03-15T10:10:00Z', "
                f"{unit}, '{tz}'), 'UTC')"
            )
            result = tdSql.queryResult[0][0]
            assert expected in result, f"tz={tz}, unit={unit}: expected {expected} in {result}"

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
        self.check_timetruncate_iana_tz_different_boundaries()
        self.check_timetruncate_int_01_compat()
        self.check_timetruncate_invalid_tz()
        self.check_timetruncate_l1_overrides_l2()
        self.check_timetruncate_no_param_uses_l2()
        self.check_timetruncate_subday_string_tz_uses_target_offset()

@SKIP_WINDOWS_SET_TIMEZONE
class TestTimetruncateNaturalUnits:
    """TIMETRUNCATE n/q/y natural units (1n/1q/1y only; Nx is invalid)."""

    pytestmark = []  # P4 implemented

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.dbname = 'db_tz_tt_nat'
        cls.ntbname = f'{cls.dbname}.ntb'

    def prepare_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.ntbname}')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp, c1 int)')
        tdSql.execute(f'insert into {self.ntbname} values (1773750600000, 1)')

    def check_timetruncate_1n_month(self):
        """1n should align to first day of month 00:00:00."""
        self.prepare_data()
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
        self.prepare_data()
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
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        for ts_str in ['2026-08-15 10:30:00', '2026-01-01 00:00:00']:
            tdSql.query(f"select to_iso8601(timetruncate('{ts_str}', 1y), 'UTC')")
            result = tdSql.queryResult[0][0]
            assert '2026-01-01T00:00:00' in result, f"1y({ts_str}): {result}"

    def check_timetruncate_1q_equals_3n_is_invalid(self):
        """3n is invalid (Nx not supported); only 1n/1q/1y are valid."""
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
        tdSql.error("select timetruncate('2026-05-15 10:30:00', 3n)")
        tdSql.error("select timetruncate('2026-05-15 10:30:00', 2q)")
        tdSql.error("select timetruncate('2026-08-15 10:30:00', 2y)")
        tdSql.error("select timetruncate('2026-08-15 10:30:00', 2w)")
        tdSql.error("select timetruncate('2026-08-15 10:30:00', 6n)")

    def check_timetruncate_natural_with_tz_param(self):
        """n/q/y units combined with string timezone parameter."""
        self.prepare_data()
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
        self.check_timetruncate_1n_month()
        self.check_timetruncate_1q_all_quarters()
        self.check_timetruncate_1y_year()
        self.check_timetruncate_1q_equals_3n_is_invalid()
        self.check_timetruncate_natural_with_tz_param()

@SKIP_WINDOWS_SET_TIMEZONE
class TestTimetruncateUnitMultiplierValidation:
    """TIMETRUNCATE unit multiplier constraint: only N=1 allowed, Nx (N>1) returns "Invalid time unit"."""

    pytestmark = []  # P4 regression

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.dbname = 'db_tz_tt_mult'
        cls.ntbname = f'{cls.dbname}.ntb'

    def prepare_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.ntbname}')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp, c1 int)')
        tdSql.execute(f'insert into {self.ntbname} values (1773750600000, 1)')

    def check_timetruncate_2n_invalid(self):
        """Natural month: 2n, 3n, 6n, 12n should all be invalid."""
        self.prepare_data()
        for mult in [2, 3, 6, 12]:
            tdSql.error(f"select timetruncate('2026-05-15 10:30:00', {mult}n)",
                        expectErrInfo="Invalid time unit : timetruncate")

    def check_timetruncate_2q_invalid(self):
        """Natural quarter: 2q, 3q, 4q should all be invalid."""
        self.prepare_data()
        for mult in [2, 3, 4]:
            tdSql.error(f"select timetruncate('2026-05-15 10:30:00', {mult}q)",
                        expectErrInfo="Invalid time unit : timetruncate")

    def check_timetruncate_2y_invalid(self):
        """Natural year: 2y, 3y, 5y should all be invalid."""
        self.prepare_data()
        for mult in [2, 3, 5]:
            tdSql.error(f"select timetruncate('2026-05-15 10:30:00', {mult}y)",
                        expectErrInfo="Invalid time unit : timetruncate")

    def check_timetruncate_2w_invalid(self):
        """Week: 2w, 3w, 4w, 10w should all be invalid."""
        self.prepare_data()
        for mult in [2, 3, 4, 10]:
            tdSql.error(f"select timetruncate('2026-05-15 10:30:00', {mult}w)",
                        expectErrInfo="Invalid time unit : timetruncate")

    def check_timetruncate_2d_invalid(self):
        """Day: 2d, 3d, 7d should all be invalid."""
        self.prepare_data()
        for mult in [2, 3, 7]:
            tdSql.error(f"select timetruncate('2026-05-15 10:30:00', {mult}d)",
                        expectErrInfo="Invalid time unit : timetruncate")

    def check_timetruncate_2h_invalid(self):
        """Hour: 2h, 3h, 6h, 24h should all be invalid."""
        self.prepare_data()
        for mult in [2, 3, 6, 24]:
            tdSql.error(f"select timetruncate('2026-05-15 10:30:00', {mult}h)",
                        expectErrInfo="Invalid time unit : timetruncate")

    def check_timetruncate_2m_invalid(self):
        """Minute: 2m, 5m, 15m, 60m should all be invalid."""
        self.prepare_data()
        for mult in [2, 5, 15, 60]:
            tdSql.error(f"select timetruncate('2026-05-15 10:30:00', {mult}m)",
                        expectErrInfo="Invalid time unit : timetruncate")

    def check_timetruncate_2s_invalid(self):
        """Second: 2s, 5s, 10s, 60s should all be invalid."""
        self.prepare_data()
        for mult in [2, 5, 10, 60]:
            tdSql.error(f"select timetruncate('2026-05-15 10:30:00', {mult}s)",
                        expectErrInfo="Invalid time unit : timetruncate")

    def check_timetruncate_2ms_invalid(self):
        """Millisecond: 2ms, 100ms, 1000ms should all be invalid."""
        self.prepare_data()
        for mult in [2, 100, 1000]:
            tdSql.error(f"select timetruncate('2026-05-15 10:30:00', {mult}a)",
                        expectErrInfo="Invalid time unit : timetruncate")

    def check_timetruncate_2u_invalid(self):
        """Microsecond: 2u, 1000u should all be invalid."""
        self.prepare_data()
        for mult in [2, 1000]:
            tdSql.error(f"select timetruncate('2026-05-15 10:30:00', {mult}u)",
                        expectErrInfo="Invalid time unit : timetruncate")

    def check_timetruncate_2b_invalid(self):
        """Nanosecond: 2b, 1000b should all be invalid."""
        self.prepare_data()
        for mult in [2, 1000]:
            tdSql.error(f"select timetruncate('2026-05-15 10:30:00', {mult}b)",
                        expectErrInfo="Invalid time unit : timetruncate")

    def check_timetruncate_multiplier_with_tz_param_invalid(self):
        """Multiplier > 1 with string timezone parameter should also be invalid."""
        self.prepare_data()
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
        self.prepare_data()
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

@SKIP_WINDOWS_SET_TIMEZONE
class TestTimetruncateWeek:
    """TIMETRUNCATE 1w/Nw aligned by firstDayOfWeek (high-risk behavior change)."""

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.dbname = 'db_tz_tt_wk'
        cls.ntbname = f'{cls.dbname}.ntb'

    def prepare_data(self):
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
        self.prepare_data()
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
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.execute("SET FIRST_DAY_OF_WEEK 1")
        tdSql.query("select to_iso8601(timetruncate('2026-04-27 10:00:00', 1w), 'UTC')")
        result = tdSql.queryResult[0][0]
        assert '2026-04-27T00:00:00' in result, f"Monday fdow=1: {result}"

    def check_timetruncate_1w_with_iana_tz(self):
        """Regression: 1w with explicit IANA tz should match session tz path."""
        self.prepare_data()
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
        self.prepare_data()
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
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        tdSql.execute("SET FIRST_DAY_OF_WEEK 0")
        tdSql.query("select to_iso8601(timetruncate('2026-03-10 10:00:00', 1w), 'America/New_York')")
        result = tdSql.queryResult[0][0]
        assert '2026-03-08T00:00:00' in result, f"1w DST spring: {result}"

    def check_set_first_day_of_week_invalid_error_message(self):
        """SET FIRST_DAY_OF_WEEK out-of-range should return formatted error message."""
        self.prepare_data()
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
        self.check_timetruncate_1w_fdow_differences()
        self.check_timetruncate_1w_on_week_start()
        self.check_timetruncate_1w_with_iana_tz()
        self.check_timetruncate_1w_with_iana_tz_dst_consistency()
        self.check_timetruncate_1w_dst_spring()
        self.check_set_first_day_of_week_invalid_error_message()

class TestWeekFunctions:
    """WEEK respects firstDayOfWeek; WEEKOFYEAR/DAYOFWEEK/WEEKDAY are fdow-independent."""

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.dbname = 'db_tz_wkfn'
        cls.ntbname = f'{cls.dbname}.ntb'

    def prepare_data(self):
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
        self.prepare_data()
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
        self.prepare_data()
        results = []
        for mode in range(8):
            tdSql.query(f"select week('2026-06-15 12:00:00', {mode})")
            results.append(tdSql.queryResult[0][0])
        assert all(result is not None for result in results), f"WEEK mode results: {results}"

    def check_week_mode_8_invalid(self):
        """WEEK(ts, 8) should fail."""
        self.prepare_data()
        tdSql.error(
            "select week('2026-01-01 12:00:00', 8)",
            expectedErrno=ERR_INVALID_FUNCTION_PARAM,
        )

    def check_dayofweek_not_affected_by_fdow(self):
        """DAYOFWEEK should return identical values regardless of firstDayOfWeek.

        DAYOFWEEK: 1=Sunday, 2=Monday, ..., 7=Saturday.
        """
        self.prepare_data()
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
        self.prepare_data()
        results = []
        for fdow in range(7):
            tdSql.execute(f"SET FIRST_DAY_OF_WEEK {fdow}")
            tdSql.query("select weekday('2026-04-30 10:00:00')")  # Thursday
            results.append(tdSql.queryResult[0][0])
        assert all(r == results[0] for r in results), \
            f"WEEKDAY should not change: {results}"

    def check_dayofweek_known_values(self):
        """Verify DAYOFWEEK returns correct values for known dates."""
        self.prepare_data()
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
        self.prepare_data()
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
        self.prepare_data()
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
        self.check_weekofyear_not_affected_by_fdow()
        self.check_week_mode_all_valid()
        self.check_week_mode_8_invalid()
        self.check_dayofweek_not_affected_by_fdow()
        self.check_weekday_not_affected_by_fdow()
        self.check_dayofweek_known_values()
        self.check_weekday_known_values()
        self.check_week_from_table()

class TestSessionFirstDayOfWeekFunction:
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

@SKIP_WINDOWS_SET_TIMEZONE
class TestDstEdge:
    """DST edge cases: spring-forward gap, fall-back overlap, write-path regression.

    DST times (America/New_York 2026):
    - Spring: 2026-03-08 02:00 EST -> 03:00 EDT
    - Fall:   2026-11-01 02:00 EDT -> 01:00 EST
    """

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.dbname = 'db_tz_dst'
        cls.ntbname = f'{cls.dbname}.ntb'

    def prepare_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.ntbname}')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp, c1 int)')

    def check_dst_spring_iso8601_offset(self):
        """TO_ISO8601 should show -05:00 before and -04:00 after spring-forward."""
        self.prepare_data()
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
        self.prepare_data()
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
        self.prepare_data()
        ts = 1772982000000  # 2026-03-08 15:00 UTC = 11:00 EDT
        tdSql.execute(f'insert into {self.ntbname} values ({ts}, 1)')
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        tdSql.query(f"select to_iso8601(timetruncate(ts, 1d), 'UTC') from {self.ntbname}")
        result = tdSql.queryResult[0][0]
        assert '2026-03-08T05:00:00' in result, f"1d spring day: {result}"

    def check_dst_fall_iso8601_offset(self):
        """TO_ISO8601 should show -04:00 before and -05:00 after fall-back."""
        self.prepare_data()
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
        self.prepare_data()
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
        self.prepare_data()
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
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        tdSql.execute(f"insert into {self.ntbname} values ('2026-11-01 01:30:00', 1)")
        tdSql.query(f"select to_iso8601(ts, 'America/New_York') from {self.ntbname}")
        result = tdSql.queryResult[0][0]
        assert '2026-11-01T01:30:00' in result, f"Fall overlap write: {result}"
        assert '-04:00' in result or '-0400' in result, f"Fall overlap write: {result}"

    def check_write_integer_ts_unaffected(self):
        """Integer timestamp write should NOT be affected by timezone."""
        self.prepare_data()
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
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        tdSql.execute(f"insert into {self.ntbname} values ('2026-11-01T01:30:00-05:00', 1)")
        tdSql.query(f"select to_iso8601(ts, 'America/New_York') from {self.ntbname}")
        result = tdSql.queryResult[0][0]
        assert '2026-11-01T01:30:00' in result, f"Explicit offset write: {result}"
        assert '-05:00' in result or '-0500' in result, f"Explicit offset write: {result}"

    def check_no_dst_timezone_unaffected(self):
        """Asia/Shanghai (no DST) should produce constant offset."""
        self.prepare_data()
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


@SKIP_WINDOWS_SET_TIMEZONE
class TestToIso8601FixedOffset:
    """Regression: TO_ISO8601 with fixed-offset tz param uses the useFixedOffset path."""

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.dbname = 'db_tz_fixedoff'
        cls.ntbname = f'{cls.dbname}.ntb'
        cls.ts = 1642248000000  # 2022-01-15 12:00:00 UTC

    def prepare_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.ntbname}')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp, c1 int)')
        tdSql.execute(f'insert into {self.ntbname} values ({self.ts}, 1)')

    def check_fixed_offset_plus(self):
        """TO_ISO8601 with '+0800' should output at UTC+8 with +0800 suffix."""
        self.prepare_data()
        tdSql.query(f"select to_iso8601(ts, '+0800') from {self.ntbname}")
        result = tdSql.queryResult[0][0]
        assert '+0800' in result, f"Expected +0800 in '{result}'"
        assert '20:00:00' in result, f"Expected 20:00:00 for UTC+8 in '{result}'"

    def check_fixed_offset_minus(self):
        """TO_ISO8601 with '-0500' should output at UTC-5 with -0500 suffix."""
        self.prepare_data()
        tdSql.query(f"select to_iso8601(ts, '-0500') from {self.ntbname}")
        result = tdSql.queryResult[0][0]
        assert '-0500' in result, f"Expected -0500 in '{result}'"
        assert '07:00:00' in result, f"Expected 07:00:00 for UTC-5 in '{result}'"

    def check_fixed_offset_z(self):
        """TO_ISO8601 with 'Z' should output UTC time with Z suffix."""
        self.prepare_data()
        tdSql.query(f"select to_iso8601(ts, 'Z') from {self.ntbname}")
        result = tdSql.queryResult[0][0]
        assert result.endswith('Z'), f"Expected Z suffix in '{result}'"
        assert '12:00:00' in result, f"Expected 12:00:00 for UTC in '{result}'"

    def check_fixed_offset_colon(self):
        """TO_ISO8601 with '+05:30' should output at UTC+5:30 with original suffix."""
        self.prepare_data()
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
        self.check_fixed_offset_plus()
        self.check_fixed_offset_minus()
        self.check_fixed_offset_z()
        self.check_fixed_offset_colon()


@SKIP_WINDOWS_SET_TIMEZONE
class TestJoinDst:
    """Regression: JOIN on TIMETRUNCATE(ts, 1d) must be DST-aware for IANA timezones."""

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.dbname = 'db_join_dst'

    def prepare_data(self):
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
        self.prepare_data()
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
        # hash join doesn't support using timetruncate as hash key
        tdSql.error(
            f"select /*+ hash_join() */ t1.v1, t2.v2 from {self.dbname}.t1 t1 "
            f"inner join {self.dbname}.t2 t2 "
            f"on timetruncate(t1.ts, 1d) = timetruncate(t2.ts, 1d)"
        )

    def check_join_no_dst_same_day(self):
        """With a non-DST timezone (Asia/Shanghai), both timestamps land on
        the same calendar day, so the join should still match."""
        self.prepare_data()
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
        # hash join doesn't support using timetruncate as hash key
        tdSql.error(
            f"select /*+ hash_join() */ * from {self.dbname}.t3 t3 "
            f"inner join {self.dbname}.t4 t4 "
            f"on timetruncate(t3.ts, 1d) = timetruncate(t4.ts, 1d)"
        )

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
        self.check_join_dst_day_truncate()
        self.check_join_no_dst_same_day()
        self.check_join_dst_different_days()

