"""Timezone config path unification tests.

Covers:
- ALTER LOCAL timezone validation (unified path via cfgSetTimezone)
- Fixed offset normalization (ISO -> POSIX UTC±h[:mm])
- Windows timezone name cross-platform acceptance
- GMT series rejection
- Ambiguous abbreviation rejection
- Error code unification (PAR_INVALID_TIMEZONE)
"""

from new_test_framework.utils import tdLog, tdSql, tdCom
import sys
import pytest

ERR_INVALID_TIMEZONE = 0x26B2

IS_WINDOWS = sys.platform.startswith('win')


def _config_timezone_name(value):
    return str(value).split(' ')[0]


class _AlterLocalTimezoneMixin:
    """ALTER LOCAL timezone validation tests (config path)."""

    def _setup_alter_local_case(self):
        tdLog.debug(f"start to execute {__file__}")
        self.dbname = 'db_tz_cfg_unify'
        self.ntbname = f'{self.dbname}.ntb'
        self.ts = 1640966400000  # 2022-01-01 00:00:00 UTC

    def _prepare_alter_local_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.ntbname}')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp, c1 int)')
        tdSql.execute(f'insert into {self.ntbname} values ({self.ts}, 1)')

    def check_alter_local_iana_names(self):
        """IANA names should be accepted via ALTER LOCAL timezone."""
        valid_iana = [
            'Asia/Shanghai',
            'America/New_York',
            'Europe/London',
            'UTC',
        ]
        for tz in valid_iana:
            tdSql.execute(f"ALTER LOCAL 'timezone {tz}'")

    def check_alter_local_fixed_offsets(self):
        """Fixed offset ISO formats should be accepted and normalized."""
        valid_offsets = [
            '+08:00',
            '+0800',
            '+08',
            '-05:00',
            '-0530',
            '-05',
            '+05:30',
            '+14:00',
            '-14:00',
        ]
        for tz in valid_offsets:
            tdSql.execute(f"ALTER LOCAL 'timezone {tz}'")

    def check_alter_local_utc_posix(self):
        """UTC±N POSIX style should be accepted."""
        valid_utc = [
            'UTC',
            'UTC-8',
            'UTC+10',
            'UTC-0',
            'UTC+0',
        ]
        for tz in valid_utc:
            tdSql.execute(f"ALTER LOCAL 'timezone {tz}'")

    def check_alter_local_utc_iso(self):
        """UTC±HH:MM ISO style should be accepted and normalized."""
        valid_utc_iso = [
            'UTC+08:00',
            'UTC-05:30',
            'UTC+0800',
            'UTC+0530',
        ]
        for tz in valid_utc_iso:
            tdSql.execute(f"ALTER LOCAL 'timezone {tz}'")

    def check_alter_local_z(self):
        """Z/z should be accepted as UTC."""
        tdSql.execute("ALTER LOCAL 'timezone Z'")
        tdSql.execute("ALTER LOCAL 'timezone z'")

    def check_alter_local_windows_names(self):
        """Windows canonical timezone names should be accepted on all platforms."""
        win_names = [
            'China Standard Time',
            'Eastern Standard Time',
            'India Standard Time',
            'Tokyo Standard Time',
        ]
        for tz in win_names:
            tdSql.execute(f"ALTER LOCAL 'timezone {tz}'")

    def check_alter_local_gmt_rejected(self):
        """GMT and GMT±N must be rejected."""
        gmt_cases = [
            'GMT',
            'GMT+8',
            'GMT-5',
            'GMT+08:00',
            'gmt',
            'Gmt+3',
        ]
        for tz in gmt_cases:
            tdSql.error(f"ALTER LOCAL 'timezone {tz}'", expectedErrno=ERR_INVALID_TIMEZONE)

    def check_alter_local_abbreviations_rejected(self):
        """Ambiguous abbreviations must be rejected."""
        abbreviations = [
            'CST',
            'EST',
            'PST',
            'IST',
        ]
        for tz in abbreviations:
            tdSql.error(f"ALTER LOCAL 'timezone {tz}'", expectedErrno=ERR_INVALID_TIMEZONE)

    def check_alter_local_single_digit_rejected(self):
        """Single-digit hour offsets must be rejected."""
        single_digit = ['+8', '-5']
        for tz in single_digit:
            tdSql.error(f"ALTER LOCAL 'timezone {tz}'", expectedErrno=ERR_INVALID_TIMEZONE)

    def check_alter_local_out_of_range_rejected(self):
        """Out-of-range offsets must be rejected."""
        out_of_range = ['+15:00', '-15:00', '+14:30']
        for tz in out_of_range:
            tdSql.error(f"ALTER LOCAL 'timezone {tz}'", expectedErrno=ERR_INVALID_TIMEZONE)

    def check_alter_local_garbage_rejected(self):
        """Garbage input must be rejected."""
        garbage = ['NotATimezone', 'abc123', '8', '']
        for tz in garbage:
            tdSql.error(f"ALTER LOCAL 'timezone {tz}'", expectedErrno=ERR_INVALID_TIMEZONE)

    def check_alter_local_fixed_offset_semantics(self):
        """ALTER LOCAL fixed offsets should apply the configured POSIX direction on new connections."""
        self._prepare_alter_local_data()

        def _get_global_timezone():
            tdSql.query("SHOW LOCAL VARIABLES LIKE 'timezone'")
            return _config_timezone_name(tdSql.queryResult[0][1])

        tdSql.connect()
        original_global = _get_global_timezone()

        cases = [
            ('+08:00', ['-08:00', '-0800']),
            ('UTC+08:00', ['-08:00', '-0800']),
            ('-05:30', ['+05:30', '+0530']),
            ('UTC-05:30', ['+05:30', '+0530']),
        ]

        try:
            for tz, expected_any in cases:
                tdSql.execute(f"ALTER LOCAL 'timezone {tz}'")
                tdSql.connect()
                tdSql.execute(f'use {self.dbname}')
                tdSql.query(f'select to_iso8601(ts) from {self.ntbname}')
                result = tdSql.queryResult[0][0]
                assert any(expected in result for expected in expected_any), \
                    f"ALTER LOCAL timezone {tz}: expected one of {expected_any} in {result}"
        finally:
            tdSql.execute(f"ALTER LOCAL 'timezone {original_global}'")
            tdSql.connect()


class TestTimezoneConfigUnification(_AlterLocalTimezoneMixin):
    """
    Title: Timezone config path unification — ALTER LOCAL validation

    Description:
        Verify that ALTER LOCAL timezone applies the same validation and
        normalization as SET TIMEZONE: accepts IANA names, fixed offsets,
        UTC±N, Windows names; rejects GMT series, abbreviations, garbage.

    Since: v3.4.0.0

    Labels: timezone, config

    Jira: None

    Catalog:
        - Function:timezone

    History:
        - 2026-05-23: Tony Zhang created

    """

    def test_alter_local_timezone(self):
        self._setup_alter_local_case()
        self.check_alter_local_iana_names()
        self.check_alter_local_fixed_offsets()
        self.check_alter_local_utc_posix()
        self.check_alter_local_utc_iso()
        self.check_alter_local_fixed_offset_semantics()
        self.check_alter_local_z()
        self.check_alter_local_windows_names()
        self.check_alter_local_gmt_rejected()
        self.check_alter_local_abbreviations_rejected()
        self.check_alter_local_single_digit_rejected()
        self.check_alter_local_out_of_range_rejected()
        self.check_alter_local_garbage_rejected()
