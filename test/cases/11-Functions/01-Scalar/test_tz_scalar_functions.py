"""Timezone scalar function tests.

Covers:
- P3 Task 3.1: TO_ISO8601(ts, 'IANA_name') IANA extension, DST, fallback L2->L3->L5
- P3 Task 3.2: TO_CHAR(ts, fmt, 'timezone') third parameter
- P3 Task 3.3: TIMETRUNCATE(ts, unit, 'tz_string') third parameter extension
- P4 Task 4.1: TIMETRUNCATE n/q/y natural units and multiples (3n/2q/2y/6n/4n)
- P4 Task 4.2: TIMETRUNCATE 1w aligned by firstDayOfWeek (high-risk change)
- P4 Task 4.4: WEEK/WEEKOFYEAR respect firstDayOfWeek; DAYOFWEEK/WEEKDAY unchanged
- DST edge cases: spring-forward gap, fall-back overlap, write-path regression
"""

from new_test_framework.utils import tdLog, tdSql


ERR_INVALID_TIMEZONE = 0x26B2
ERR_INVALID_FUNCTION_PARAM = 0x2803


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

    def test_to_iso8601_iana_no_dst(self):
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

    def test_to_iso8601_iana_dst_new_york(self):
        """America/New_York: winter -05:00 (EST), summer -04:00 (EDT)."""
        self.prepare_data()
        tdSql.query(
            f"select to_iso8601(ts, 'America/New_York') from {self.ntbname} order by ts"
        )
        winter, summer = tdSql.queryResult[0][0], tdSql.queryResult[1][0]
        assert '-05:00' in winter or '-0500' in winter, f"Winter: {winter}"
        assert '-04:00' in summer or '-0400' in summer, f"Summer: {summer}"

    def test_to_iso8601_iana_dst_london(self):
        """Europe/London: winter +00:00 (GMT), summer +01:00 (BST)."""
        self.prepare_data()
        tdSql.query(
            f"select to_iso8601(ts, 'Europe/London') from {self.ntbname} order by ts"
        )
        winter, summer = tdSql.queryResult[0][0], tdSql.queryResult[1][0]
        assert any(e in winter for e in ['+00:00', '+0000', 'Z'])
        assert '+01:00' in summer or '+0100' in summer, f"Summer: {summer}"

    def test_to_iso8601_fixed_offset_compat(self):
        """Fixed offset params (+08:00, Z) should still work (backward compat)."""
        self.prepare_data()
        tdSql.query(f"select to_iso8601(ts, '+08:00') from {self.ntbname} order by ts")
        for row in tdSql.queryResult:
            assert '+08:00' in row[0] or '+0800' in row[0]

    def test_to_iso8601_invalid_tz(self):
        """Invalid timezone params should fail."""
        self.prepare_data()
        for tz in ["'Invalid/Zone'", "'CST'", "'+8'"]:
            tdSql.error(
                f"select to_iso8601(ts, {tz}) from {self.ntbname}",
                expectedErrno=ERR_INVALID_TIMEZONE,
            )

    def test_to_iso8601_no_param_uses_l2(self):
        """TO_ISO8601(ts) without tz param should use connection timezone (L2)."""
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        tdSql.query(f"select to_iso8601(ts) from {self.ntbname} order by ts")
        winter, summer = tdSql.queryResult[0][0], tdSql.queryResult[1][0]
        assert '-05:00' in winter or '-0500' in winter, f"L2 winter: {winter}"
        assert '-04:00' in summer or '-0400' in summer, f"L2 summer: {summer}"

    def test_to_iso8601_l1_overrides_l2(self):
        """Explicit tz param (L1) should override connection timezone (L2)."""
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
        tdSql.query(
            f"select to_iso8601(ts, 'America/New_York') from {self.ntbname} where c1 = 1"
        )
        assert '-05:00' in tdSql.queryResult[0][0] or '-0500' in tdSql.queryResult[0][0]

    def test_to_iso8601_us_ns_precision(self):
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

    def test_to_char_iana_and_fixed_offset(self):
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

    def test_to_char_invalid_tz(self):
        """Invalid timezone params should fail."""
        self.prepare_data()
        fmt = 'YYYY-MM-DD HH24:MI:SS'
        for tz in ["'Invalid/Zone'", "'CST'"]:
            tdSql.error(
                f"select to_char(ts, '{fmt}', {tz}) from {self.ntbname}",
                expectedErrno=ERR_INVALID_TIMEZONE,
            )

    def test_to_char_no_param_uses_l2(self):
        """TO_CHAR without tz param should use connection timezone (L2)."""
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        tdSql.query(
            f"select to_char(ts, 'YYYY-MM-DD HH24:MI:SS') from {self.ntbname} where c1 = 1"
        )
        assert '07:00:00' in tdSql.queryResult[0][0]

    def test_to_char_l1_overrides_l2(self):
        """Explicit tz (L1) should override connection timezone (L2)."""
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
        tdSql.query(
            f"select to_char(ts, 'YYYY-MM-DD HH24:MI:SS', 'America/New_York') "
            f"from {self.ntbname} where c1 = 1"
        )
        assert '07:00:00' in tdSql.queryResult[0][0]

    def test_to_char_two_params_compat(self):
        """TO_CHAR(ts, fmt) two-param form should still work."""
        self.prepare_data()
        tdSql.query(
            f"select to_char(ts, 'YYYY-MM-DD HH24:MI:SS') from {self.ntbname} where c1 = 1"
        )
        assert '2022' in tdSql.queryResult[0][0]


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

    def test_timetruncate_iana_tz_different_boundaries(self):
        """String tz param should truncate in that timezone's calendar.

        UTC vs Shanghai should produce different 1d boundaries for this timestamp.
        """
        self.prepare_data()
        tdSql.query(f"select timetruncate(ts, 1d, 'UTC') from {self.ntbname}")
        r_utc = tdSql.queryResult[0][0]
        tdSql.query(f"select timetruncate(ts, 1d, 'Asia/Shanghai') from {self.ntbname}")
        r_sh = tdSql.queryResult[0][0]
        assert r_utc != r_sh, f"UTC vs Shanghai 1d should differ: {r_utc} vs {r_sh}"

    def test_timetruncate_int_01_compat(self):
        """Integer 0/1 and no-param forms should work (backward compat)."""
        self.prepare_data()
        for p in ['0', '1', '']:
            suffix = f', {p}' if p else ''
            tdSql.query(f"select timetruncate(ts, 1d{suffix}) from {self.ntbname}")
            assert tdSql.queryResult[0][0] is not None

    def test_timetruncate_invalid_tz(self):
        """Invalid timezone string should fail."""
        self.prepare_data()
        for tz in ["'Invalid/Zone'", "'CST'"]:
            tdSql.error(
                f"select timetruncate(ts, 1d, {tz}) from {self.ntbname}",
                expectedErrno=ERR_INVALID_TIMEZONE,
            )

    def test_timetruncate_l1_overrides_l2(self):
        """Explicit tz (L1) should override connection timezone (L2)."""
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
        tdSql.query(f"select timetruncate(ts, 1d, 'UTC') from {self.ntbname}")
        r_utc = tdSql.queryResult[0][0]
        tdSql.query(f"select timetruncate(ts, 1d, 'Asia/Shanghai') from {self.ntbname}")
        r_sh = tdSql.queryResult[0][0]
        assert r_utc != r_sh

    def test_timetruncate_no_param_uses_l2(self):
        """Without tz param, different L2 should produce different results."""
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        tdSql.query(f"select timetruncate(ts, 1d) from {self.ntbname}")
        r_ny = tdSql.queryResult[0][0]
        tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
        tdSql.query(f"select timetruncate(ts, 1d) from {self.ntbname}")
        r_sh = tdSql.queryResult[0][0]
        assert r_ny != r_sh


class TestTimetruncateNaturalUnits:
    """TIMETRUNCATE n/q/y units and multiples (3n/2q/2y/6n/2w etc.)."""

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

    def test_timetruncate_1n_month(self):
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

    def test_timetruncate_1q_all_quarters(self):
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

    def test_timetruncate_1y_year(self):
        """1y should align to Jan 1 00:00:00."""
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        for ts_str in ['2026-08-15 10:30:00', '2026-01-01 00:00:00']:
            tdSql.query(f"select to_iso8601(timetruncate('{ts_str}', 1y), 'UTC')")
            result = tdSql.queryResult[0][0]
            assert '2026-01-01T00:00:00' in result, f"1y({ts_str}): {result}"

    def test_timetruncate_1q_equals_3n(self):
        """1q and 3n should produce identical results for any timestamp."""
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
        test_dates = [
            '2026-02-15 10:30:00', '2026-05-15 10:30:00',
            '2026-08-15 10:30:00', '2026-11-15 10:30:00',
        ]
        for ts_str in test_dates:
            tdSql.query(f"select timetruncate('{ts_str}', 1q)")
            r_q = tdSql.queryResult[0][0]
            tdSql.query(f"select timetruncate('{ts_str}', 3n)")
            r_n = tdSql.queryResult[0][0]
            assert r_q == r_n, f"1q vs 3n at {ts_str}: {r_q} vs {r_n}"

    def test_timetruncate_multiples(self):
        """Multi-unit truncation: 2n, 3n, 6n, 2q, 2y, 5y, 4n."""
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        expected = {
            '2n': '2026-07-01T00:00:00',
            '3n': '2026-07-01T00:00:00',
            '6n': '2026-07-01T00:00:00',
            '2q': '2026-07-01T00:00:00',
            '2y': '2026-01-01T00:00:00',
            '5y': '2025-01-01T00:00:00',
            '4n': '2026-05-01T00:00:00',
        }
        for unit, expected_start in expected.items():
            tdSql.query(f"select to_iso8601(timetruncate('2026-08-15 10:30:00', {unit}), 'UTC')")
            result = tdSql.queryResult[0][0]
            assert expected_start in result, f"{unit}: expected {expected_start} in {result}"

    def test_timetruncate_2q_equals_6n(self):
        """2q should be equivalent to 6n."""
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        for ts_str in ['2026-03-15 10:30:00', '2026-08-15 10:30:00', '2026-12-15 10:30:00']:
            tdSql.query(f"select timetruncate('{ts_str}', 2q)")
            r_2q = tdSql.queryResult[0][0]
            tdSql.query(f"select timetruncate('{ts_str}', 6n)")
            r_6n = tdSql.queryResult[0][0]
            assert r_2q == r_6n, f"2q vs 6n at {ts_str}: {r_2q} vs {r_6n}"

    def test_timetruncate_natural_with_tz_param(self):
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

    def test_timetruncate_1w_fdow_differences(self):
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

    def test_timetruncate_1w_on_week_start(self):
        """Truncating on exact week start day should return that day's midnight."""
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.execute("SET FIRST_DAY_OF_WEEK 1")
        tdSql.query("select to_iso8601(timetruncate('2026-04-27 10:00:00', 1w), 'UTC')")
        result = tdSql.queryResult[0][0]
        assert '2026-04-27T00:00:00' in result, f"Monday fdow=1: {result}"

    def test_timetruncate_2w_multi_week(self):
        """2w should align by 2-week intervals respecting firstDayOfWeek."""
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        results = {}
        for fdow in [0, 1]:
            tdSql.execute(f"SET FIRST_DAY_OF_WEEK {fdow}")
            tdSql.query("select to_iso8601(timetruncate('2026-04-30 10:00:00', 2w), 'UTC')")
            results[fdow] = tdSql.queryResult[0][0]
            assert 'T00:00:00' in results[fdow], f"2w fdow={fdow}: {results[fdow]}"
        assert results[0] != results[1], f"2w should respect fdow: {results}"

    def test_timetruncate_1w_with_iana_tz(self):
        """1w with IANA timezone param."""
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

    def test_timetruncate_1w_dst_spring(self):
        """1w during DST spring-forward should still align to local 00:00:00."""
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        tdSql.execute("SET FIRST_DAY_OF_WEEK 0")
        tdSql.query("select to_iso8601(timetruncate('2026-03-10 10:00:00', 1w), 'America/New_York')")
        result = tdSql.queryResult[0][0]
        assert '2026-03-08T00:00:00' in result, f"1w DST spring: {result}"


class TestWeekFunctions:
    """WEEK/WEEKOFYEAR respect firstDayOfWeek; DAYOFWEEK/WEEKDAY unchanged."""

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

    def test_weekofyear_respects_fdow(self):
        """WEEKOFYEAR with different firstDayOfWeek should succeed."""
        self.prepare_data()
        results = {}
        for fdow in [0, 1]:
            tdSql.execute(f"SET FIRST_DAY_OF_WEEK {fdow}")
            tdSql.query("select weekofyear('2026-01-01 12:00:00')")
            results[fdow] = tdSql.queryResult[0][0]
        assert results[0] != results[1], f"WEEKOFYEAR should respect fdow: {results}"

    def test_week_mode_all_valid(self):
        """WEEK(ts, mode) for mode 0-7 should all succeed."""
        self.prepare_data()
        results = []
        for mode in range(8):
            tdSql.query(f"select week('2026-06-15 12:00:00', {mode})")
            results.append(tdSql.queryResult[0][0])
        assert all(result is not None for result in results), f"WEEK mode results: {results}"

    def test_week_mode_8_invalid(self):
        """WEEK(ts, 8) should fail."""
        self.prepare_data()
        tdSql.error(
            "select week('2026-01-01 12:00:00', 8)",
            expectedErrno=ERR_INVALID_FUNCTION_PARAM,
        )

    def test_week_mode_overrides_fdow(self):
        """WEEK(ts, mode) mode is L1 and should override firstDayOfWeek."""
        self.prepare_data()
        tdSql.execute("SET FIRST_DAY_OF_WEEK 1")
        tdSql.query("select week('2026-01-04 12:00:00', 0)")  # mode=0: Sunday start
        r0 = tdSql.queryResult[0][0]
        tdSql.query("select week('2026-01-04 12:00:00', 1)")  # mode=1: Monday start
        r1 = tdSql.queryResult[0][0]
        assert r0 != r1, f"WEEK mode should override fdow: mode=0 {r0}, mode=1 {r1}"

    def test_dayofweek_not_affected_by_fdow(self):
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

    def test_weekday_not_affected_by_fdow(self):
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

    def test_dayofweek_known_values(self):
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

    def test_weekday_known_values(self):
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

    def test_week_from_table(self):
        """WEEK/WEEKOFYEAR should work with table data."""
        self.prepare_data()
        tdSql.query(f"select week(ts), weekofyear(ts) from {self.ntbname}")
        tdSql.checkRows(1)


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

    def test_dst_spring_iso8601_offset(self):
        """TO_ISO8601 should show -05:00 before and -04:00 after spring-forward."""
        self.prepare_data()
        ts_before = 1741413600000  # 2026-03-08 01:00 EST = 06:00 UTC
        ts_after = 1741417200000   # 2026-03-08 03:00 EDT = 07:00 UTC
        tdSql.execute(f'insert into {self.ntbname} values ({ts_before}, 1)')
        tdSql.execute(f'insert into {self.ntbname} values ({ts_after}, 2)')

        tdSql.query(
            f"select to_iso8601(ts, 'America/New_York') from {self.ntbname} order by ts"
        )
        before, after = tdSql.queryResult[0][0], tdSql.queryResult[1][0]
        assert '-05:00' in before or '-0500' in before, f"Before spring: {before}"
        assert '-04:00' in after or '-0400' in after, f"After spring: {after}"

    def test_dst_spring_to_char_gap(self):
        """TO_CHAR should show 01:59 before gap and 03:00 after gap (no 02:xx exists)."""
        self.prepare_data()
        ts_before_gap = 1741413540000  # 01:59 EST = 06:59 UTC
        ts_after_gap = 1741417200000   # 03:00 EDT = 07:00 UTC
        tdSql.execute(f'insert into {self.ntbname} values ({ts_before_gap}, 1)')
        tdSql.execute(f'insert into {self.ntbname} values ({ts_after_gap}, 2)')

        tdSql.query(
            f"select to_char(ts, 'YYYY-MM-DD HH24:MI:SS', 'America/New_York') "
            f"from {self.ntbname} order by ts"
        )
        assert '01:59:00' in tdSql.queryResult[0][0]
        assert '03:00:00' in tdSql.queryResult[1][0]

    def test_dst_spring_timetruncate_1d(self):
        """TIMETRUNCATE(1d) on spring-forward day should align to local midnight."""
        self.prepare_data()
        ts = 1741446000000  # 2026-03-08 15:00 UTC = 11:00 EDT
        tdSql.execute(f'insert into {self.ntbname} values ({ts}, 1)')
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        tdSql.query(f"select to_iso8601(timetruncate(ts, 1d), 'UTC') from {self.ntbname}")
        result = tdSql.queryResult[0][0]
        assert '2026-03-08T05:00:00' in result, f"1d spring day: {result}"

    def test_dst_fall_iso8601_offset(self):
        """TO_ISO8601 should show -04:00 before and -05:00 after fall-back."""
        self.prepare_data()
        ts_edt = 1762056000000  # 01:00 EDT = 05:00 UTC
        ts_est = 1762059600000  # 01:00 EST = 06:00 UTC
        tdSql.execute(f'insert into {self.ntbname} values ({ts_edt}, 1)')
        tdSql.execute(f'insert into {self.ntbname} values ({ts_est}, 2)')

        tdSql.query(
            f"select to_iso8601(ts, 'America/New_York') from {self.ntbname} order by ts"
        )
        before, after = tdSql.queryResult[0][0], tdSql.queryResult[1][0]
        assert '-04:00' in before or '-0400' in before, f"Before fall: {before}"
        assert '-05:00' in after or '-0500' in after, f"After fall: {after}"

    def test_dst_fall_timetruncate_1d(self):
        """TIMETRUNCATE(1d) on fall-back day (25h long) should align to local midnight."""
        self.prepare_data()
        ts = 1762095600000  # 2026-11-01 12:00 EST = 17:00 UTC
        tdSql.execute(f'insert into {self.ntbname} values ({ts}, 1)')
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        tdSql.query(f"select to_iso8601(timetruncate(ts, 1d), 'UTC') from {self.ntbname}")
        result = tdSql.queryResult[0][0]
        assert '2026-11-01T04:00:00' in result, f"1d fall-back day: {result}"

    def test_dst_spring_write_gap_normalized(self):
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

    def test_dst_fall_write_overlap_first_occurrence(self):
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

    def test_write_integer_ts_unaffected(self):
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

    def test_write_with_explicit_offset(self):
        """Writing timestamp with explicit offset should be unambiguous."""
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        tdSql.execute(f"insert into {self.ntbname} values ('2026-11-01T01:30:00-05:00', 1)")
        tdSql.query(f"select to_iso8601(ts, 'America/New_York') from {self.ntbname}")
        result = tdSql.queryResult[0][0]
        assert '2026-11-01T01:30:00' in result, f"Explicit offset write: {result}"
        assert '-05:00' in result or '-0500' in result, f"Explicit offset write: {result}"

    def test_no_dst_timezone_unaffected(self):
        """Asia/Shanghai (no DST) should produce constant offset."""
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
        tdSql.query("select to_iso8601('2026-03-08 10:00:00', 'Asia/Shanghai')")
        assert '+08:00' in tdSql.queryResult[0][0] or '+0800' in tdSql.queryResult[0][0]
