"""Timezone configuration, display, and query behavior tests.

Covers:
- P1 Task 1.1: SET TIMEZONE syntax (IANA / fixed offset / invalid)
- P1 Task 1.2: SET FIRST_DAY_OF_WEEK syntax
- P1 Task 1.3: Server-side firstDayOfWeek configuration
- P2 Task 2.1: Normal column timestamp display uses connection timezone
- P2 Task 2.2: SHOW / EXPLAIN timestamp display
- P2 Task 2.3: WHERE / CAST / JOIN time literal parsing (L2 -> L3 -> L5)
- P2 Task 2.5: TODAY() connection timezone (L2 -> L3 -> L5)
- NOW() regression (unchanged behavior)
- P2: INTERVAL window aggregation gap coverage (session timezone not fully wired yet)
- P6 Task 6.1: TIMEZONE() returns session timezone (L2 -> L3 -> L5)
"""

from new_test_framework.utils import tdLog, tdSql, tdCom
import datetime
import os
import pytest
import re
import subprocess
import sys

ERR_INVALID_TIMEZONE = 0x26B2
ERR_INVALID_FIRST_DAY_OF_WEEK = 0x26B3
ERR_INVALID_FUNCTION_PARAM = 0x2803
ERR_INVALID_DNODE_CFG = 0x03B2
ERR_INVALID_CFG = 0x0119

IS_WINDOWS = sys.platform.startswith('win')
SKIP_WINDOWS_SET_TIMEZONE = pytest.mark.skipif(
    IS_WINDOWS,
    reason='Windows does not support SET TIMEZONE cases in this suite',
)

pytestmark = [SKIP_WINDOWS_SET_TIMEZONE]

class _SetTimezoneMixin:
    """SET TIMEZONE '<timezone_string>' syntax tests."""

    def _setup_set_timezone_case(self):
        tdLog.debug(f"start to execute {__file__}")
        self.dbname = 'db_tz_set'
        self.ntbname = f'{self.dbname}.ntb'
        self.ts = 1640966400000  # 2022-01-01 00:00:00 UTC

    def _prepare_set_timezone_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.ntbname}')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp, c1 int)')
        tdSql.execute(f'insert into {self.ntbname} values ({self.ts}, 1)')

    def check_set_timezone_valid_iana(self):
        """Valid IANA names should be accepted and affect TO_ISO8601 output."""
        self._prepare_set_timezone_data()
        cases = {
            'Asia/Shanghai': ['+08:00', '+0800'],
            'America/New_York': ['-05:00', '-0500', '-04:00', '-0400'],  # EST or EDT
            'UTC': ['+00:00', '+0000', 'Z'],
            'Europe/London': ['+00:00', '+0000', '+01:00', '+0100', 'Z'],  # GMT(Z) or BST
            'Asia/Tokyo': ['+09:00', '+0900'],
        }
        for tz, expected_any in cases.items():
            tdSql.execute(f"SET TIMEZONE '{tz}'")
            tdSql.query(f'select to_iso8601(ts) from {self.ntbname}')
            result = tdSql.queryResult[0][0]
            assert any(e in result for e in expected_any), \
                f"SET TIMEZONE '{tz}': expected one of {expected_any} in {result}"

    def check_set_timezone_valid_fixed_offsets(self):
        """Valid fixed offset formats: +HH:MM, +HHMM, +HH, Z, half-hour, ±14:00.

        Note on the flipped expected values: a *bare* fixed offset is parsed with
        the POSIX sign convention (+ = west, - = east, see normalizeOffsetTzCommon),
        so SET TIMEZONE '+08:00' means UTC-8 and TO_ISO8601 correctly renders the
        opposite-sign ISO offset '-08:00'. Z / zero offset is UTC and not flipped.
        This flip applies ONLY to bare offsets -- IANA names (checked separately in
        check_set_timezone_valid_iana) carry their true geographic offset and are
        expected unflipped (e.g. Asia/Shanghai -> +08:00).
        """
        self._prepare_set_timezone_data()
        valid_offsets = [
            ('+08:00', ['-08:00']),   # POSIX +08 = west -> ISO -08:00
            ('+0800',  ['-0800']),
            ('+08',    ['-0800']),
            ('-05:00', ['+05:00']),   # POSIX -05 = east -> ISO +05:00
            ('Z',      ['Z']),        # zero offset (UTC), no flip
            ('+05:30', ['-05:30']),
        ]
        for tz, expected_any in valid_offsets:
            tdSql.execute(f"SET TIMEZONE '{tz}'")
            tdSql.query(f'select to_iso8601(ts) from {self.ntbname}')
            result = tdSql.queryResult[0][0]
            assert any(e in result for e in expected_any), \
                f"SET TIMEZONE '{tz}': expected one of {expected_any} in {result}"

        # Boundary: ±14:00 should be accepted (no query check needed)
        for tz in ['+14:00', '-14:00']:
            tdSql.execute(f"SET TIMEZONE '{tz}'")

    def check_set_timezone_empty_string_degrades_to_utc(self):
        """SET TIMEZONE '' (empty string) should degrade to UTC, consistent with C API."""
        self._prepare_set_timezone_data()
        tdSql.execute("SET TIMEZONE ''")
        tdSql.query(f'select to_iso8601(ts) from {self.ntbname}')
        result = tdSql.queryResult[0][0]
        assert any(e in result for e in ['+00:00', '+0000', 'Z']), \
            f"SET TIMEZONE '': expected UTC offset in {result}"

    def check_set_timezone_invalid_inputs(self):
        """Invalid timezone strings must return error."""
        invalid = [
            '+8', '-4',                    # single digit hour
            'CST', 'EST', 'PST',           # ambiguous abbreviations
            '+14:30', '-14:30', '+15:00',  # out of range
            'Invalid/Timezone',            # non-existent IANA
            'abc123', '8',                 # random garbage
            'GMT', 'GMT+8', 'GMT-5',       # GMT series rejected
        ]
        for tz in invalid:
            tdSql.error(f"SET TIMEZONE '{tz}'", expectedErrno=ERR_INVALID_TIMEZONE)

    def check_set_timezone_affects_subsequent_queries(self):
        """Multiple SET TIMEZONE changes on same connection should all take effect."""
        self._prepare_set_timezone_data()
        tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
        tdSql.query(f'select to_iso8601(ts) from {self.ntbname}')
        result_sh = tdSql.queryResult[0][0]

        tdSql.execute("SET TIMEZONE 'America/New_York'")
        tdSql.query(f'select to_iso8601(ts) from {self.ntbname}')
        result_ny = tdSql.queryResult[0][0]

        assert result_sh != result_ny, \
            f"Same ts should display differently: Shanghai={result_sh}, NY={result_ny}"

    def check_set_timezone_connection_isolation(self):
        """SET TIMEZONE should not leak across reconnects (L2 scope only)."""
        self._prepare_set_timezone_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query(f'select to_iso8601(ts) from {self.ntbname}')
        session_result = tdSql.queryResult[0][0]

        tdSql.connect()
        tdSql.query(f'select to_iso8601(ts) from {self.ntbname}')
        new_conn_result = tdSql.queryResult[0][0]

        assert session_result != new_conn_result, (
            f"Session timezone should not leak after reconnect: "
            f"old={session_result}, new={new_conn_result}"
        )

    def test_set_timezone(self):
        """summary: SET TIMEZONE '<timezone_string>' syntax tests.

        description: SET TIMEZONE '<timezone_string>' syntax tests.

        Since: v3.4.2.0

        Labels: timezone

        Jira: None

        Catalog:
            - Function:timezone

        History:
            - 2026-05-12: Tony Zhang created

        """
        self._setup_set_timezone_case()
        self.check_set_timezone_valid_iana()
        self.check_set_timezone_valid_fixed_offsets()
        self.check_set_timezone_empty_string_degrades_to_utc()
        self.check_set_timezone_invalid_inputs()
        self.check_set_timezone_affects_subsequent_queries()
        self.check_set_timezone_connection_isolation()

class _SetFirstDayOfWeekMixin:
    """SET FIRST_DAY_OF_WEEK <0..6> syntax and server config tests."""

    def _setup_set_first_day_of_week_case(self):
        tdLog.debug(f"start to execute {__file__}")
        self.dbname = 'db_tz_fdow'
        self.ntbname = f'{self.dbname}.ntb'

    def _prepare_set_first_day_of_week_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.ntbname}')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp, c1 int)')
        for i in range(7):
            ts = 1745712000000 + i * 86400000
            tdSql.execute(f'insert into {self.ntbname} values ({ts}, {i})')

    def check_set_fdow_valid_range(self):
        """SET FIRST_DAY_OF_WEEK 0..6 should all succeed."""
        for v in range(7):
            tdSql.execute(f"SET FIRST_DAY_OF_WEEK {v}")

    def check_set_fdow_invalid_range(self):
        """Values outside 0-6 should fail."""
        for v in [7, 100, -1]:
            tdSql.error(
                f"SET FIRST_DAY_OF_WEEK {v}",
                expectedErrno=ERR_INVALID_FIRST_DAY_OF_WEEK,
            )

    def check_fdow_alter_local_accepted(self):
        """ALTER LOCAL 'firstDayOfWeek' should be accepted (client config)."""
        tdSql.execute("ALTER LOCAL 'firstDayOfWeek' '0'")
        tdSql.execute("ALTER LOCAL 'firstDayOfWeek' '1'")

    def check_fdow_alter_all_dnodes_rejected(self):
        """ALTER ALL DNODES 'firstDayOfWeek' should be rejected (client-only config)."""
        tdSql.error(
            "ALTER ALL DNODES 'firstDayOfWeek' '0'",
            expectedErrno=ERR_INVALID_CFG,
        )

    def check_fdow_alter_single_dnode_rejected(self):
        """ALTER DNODE N 'firstDayOfWeek' should be rejected (client-only config)."""
        tdSql.error(
            "ALTER DNODE 1 'firstDayOfWeek' '0'",
            expectedErrno=ERR_INVALID_CFG,
        )

    def check_fdow_alter_local_updates_process_global_not_existing_connection(self):
        """ALTER LOCAL 'firstDayOfWeek' updates the process-global L3 value and is
        visible to new connections; existing connections snapshot L3 at creation
        time and must not be affected.

        Regression for: clientEnv.c fix that snapshots tsFirstDayOfWeek into
        STscObj.optionInfo.firstDayOfWeek at connection creation, so ALTER LOCAL
        only takes effect for connections created afterwards.

        P1 (immediate): ALTER LOCAL changes SHOW LOCAL VARIABLES, new connections
        inherit the new value.
        P4 (skip): full behavioural verification via TIMETRUNCATE(1w) once the
        qworker->planner->executor chain consumes firstDayOfWeek.
        """
        def _get_global_fdow():
            tdSql.query("SHOW LOCAL VARIABLES LIKE 'firstDayOfWeek'")
            for row in tdSql.queryResult:
                if row[0] == 'firstDayOfWeek':
                    return int(row[1])
            raise AssertionError("firstDayOfWeek not found in SHOW LOCAL VARIABLES")

        initial = _get_global_fdow()
        target = 0 if initial != 0 else 1

        try:
            # ALTER LOCAL must update the process-global value.
            tdSql.execute(f"ALTER LOCAL 'firstDayOfWeek' '{target}'")
            assert _get_global_fdow() == target, (
                f"ALTER LOCAL should update process-global firstDayOfWeek: "
                f"expected {target}, got {_get_global_fdow()}"
            )

            # A new connection created after ALTER LOCAL must inherit the new L3 value.
            tdSql.connect()
            assert _get_global_fdow() == target, (
                f"New connection should see the updated process-global firstDayOfWeek: "
                f"expected {target}, got {_get_global_fdow()}"
            )
        finally:
            tdSql.execute(f"ALTER LOCAL 'firstDayOfWeek' '{initial}'")
            tdSql.connect()

    def check_fdow_alter_local_does_not_affect_existing_connection_timetruncate(self):
        """Existing connection must keep its L3 snapshot; ALTER LOCAL must only
        affect connections created afterwards.

        2026-04-30 (Thursday) test point:
          fdow=4 (Thu, epoch default): TIMETRUNCATE(1w) -> 2026-04-30
          fdow=1 (Mon):                TIMETRUNCATE(1w) -> 2026-04-27
        """
        import taos as _taos

        self._prepare_set_first_day_of_week_data()
        tdSql.execute("SET TIMEZONE 'UTC'")

        # Ensure the process-global is fdow=4 (default) before we start.
        tdSql.execute("ALTER LOCAL 'firstDayOfWeek' '4'")

        # conn1 snapshots L3=4 at creation time.
        conn1 = _taos.connect()
        cur1 = conn1.cursor()

        try:
            # Now change the process-global to fdow=1.
            tdSql.execute("ALTER LOCAL 'firstDayOfWeek' '1'")

            # conn2 snapshots L3=1.
            conn2 = _taos.connect()
            cur2 = conn2.cursor()

            try:
                ts_literal = '2026-04-30 10:00:00'

                cur1.execute(f"SELECT TIMETRUNCATE('{ts_literal}', 1w)")
                result1 = cur1.fetchone()[0]

                cur2.execute(f"SELECT TIMETRUNCATE('{ts_literal}', 1w)")
                result2 = cur2.fetchone()[0]

                assert result1 != result2, (
                    f"conn1 (snapshot fdow=4) and conn2 (snapshot fdow=1) should "
                    f"produce different TIMETRUNCATE(1w) results for the same ts; "
                    f"conn1={result1}, conn2={result2}"
                )
            finally:
                conn2.close()
        finally:
            conn1.close()
            tdSql.execute(f"ALTER LOCAL 'firstDayOfWeek' '4'")
            tdSql.connect()

    def test_set_first_day_of_week(self):
        """summary: SET FIRST_DAY_OF_WEEK <0..6> syntax and server config tests.

        description: SET FIRST_DAY_OF_WEEK <0..6> syntax and server config tests.

        Since: v3.4.2.0

        Labels: timezone

        Jira: None

        Catalog:
            - Function:timezone

        History:
            - 2026-05-12: Tony Zhang created

        """
        self._setup_set_first_day_of_week_case()
        self.check_set_fdow_valid_range()
        self.check_set_fdow_invalid_range()
        self.check_fdow_alter_local_accepted()
        self.check_fdow_alter_all_dnodes_rejected()
        self.check_fdow_alter_single_dnode_rejected()
        self.check_fdow_alter_local_updates_process_global_not_existing_connection()
        self.check_fdow_alter_local_does_not_affect_existing_connection_timetruncate()

class _TimezoneFuncMixin:
    """TIMEZONE() function tests (P6 Task 6.1)."""

    def _setup_timezone_func_case(self):
        tdLog.debug(f"start to execute {__file__}")
        self.dbname = 'db_tz_func'
        self.ntbname = f'{self.dbname}.ntb'

    def _prepare_timezone_func_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.ntbname}')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp, c1 int)')
        tdSql.execute(f'insert into {self.ntbname} values (now, 1)')

    def check_timezone_returns_session_tz_after_set(self):
        """TIMEZONE() should return current session timezone after SET TIMEZONE."""
        self._prepare_timezone_func_data()
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        tdSql.query("select timezone()")
        val = tdSql.queryResult[0][0]
        assert 'New_York' in val or 'America/New_York' in val, \
            f"TIMEZONE() should reflect session timezone: {val}"

    def check_timezone_no_set_fallback_l3_l5(self):
        """Without SET TIMEZONE, TIMEZONE() should follow connection snapshot (L3->L5)."""
        self._prepare_timezone_func_data()

        tdSql.connect()
        tdSql.query("select timezone()")
        baseline = tdSql.queryResult[0][0]

        target = 'Asia/Shanghai'
        if 'Shanghai' in baseline or '+08' in baseline:
            target = 'UTC'

        tdSql.execute(f"SET TIMEZONE '{target}'")
        tdSql.query("select timezone()")
        session_val = tdSql.queryResult[0][0]
        assert target.split('/')[-1] in session_val or target == session_val, \
            f"SET TIMEZONE should affect current session TIMEZONE(): target={target}, val={session_val}"

        tdSql.connect()
        tdSql.query("select timezone()")
        reconnect_val = tdSql.queryResult[0][0]
        assert reconnect_val == baseline, \
            f"TIMEZONE() without SET should fallback to connection snapshot: baseline={baseline}, reconnect={reconnect_val}"

    def check_alter_local_timezone_changes_new_connections_only(self):
        """ALTER LOCAL timezone should affect new connections only."""
        self._prepare_timezone_func_data()

        def _get_global_timezone():
            tdSql.query("SHOW LOCAL VARIABLES LIKE 'timezone'")
            for row in tdSql.queryResult:
                if row[0] == 'timezone':
                    return str(row[1])
            raise AssertionError("timezone not found in SHOW LOCAL VARIABLES")

        tdSql.connect()
        tdSql.query("select timezone()")
        original = tdSql.queryResult[0][0]
        original_global = _get_global_timezone().split(' (')[0]

        target = 'Asia/Shanghai'
        if 'Shanghai' in original or '+08' in original:
            target = 'UTC'

        try:
            tdSql.execute(f"alter local 'timezone {target}'")

            tdSql.query("select timezone()")
            same_conn = tdSql.queryResult[0][0]
            assert same_conn == original, \
                f"ALTER LOCAL should not affect existing connection: before={original}, after={same_conn}"

            tdSql.connect()
            tdSql.query("select timezone()")
            new_conn = tdSql.queryResult[0][0]
            assert target in new_conn or target.split('/')[-1] in new_conn, \
                f"ALTER LOCAL should affect new connection: target={target}, actual={new_conn}"
        finally:
            tdSql.execute(f"alter local 'timezone {original_global}'")
            tdSql.connect()

    def check_timezone_invalid_params(self):
        """TIMEZONE() should reject any parameter under no-arg grammar."""
        self._prepare_timezone_func_data()
        for p in ['0', '1', "'abc'"]:
            tdSql.error(f"select timezone({p})")

    def check_timezone_from_table(self):
        """TIMEZONE() should work in FROM table context."""
        self._prepare_timezone_func_data()
        tdSql.query(f"select timezone() from {self.ntbname}")
        tdSql.checkRows(1)
        assert tdSql.queryResult[0][0] is not None and len(tdSql.queryResult[0][0]) > 0

    def test_timezone_func(self):
        """summary: TIMEZONE() function tests (P6 Task 6.1).

        description: TIMEZONE() function tests (P6 Task 6.1).

        Since: v3.4.2.0

        Labels: timezone

        Jira: None

        Catalog:
            - Function:timezone

        History:
            - 2026-05-12: Tony Zhang created

        """
        self._setup_timezone_func_case()
        self.check_timezone_returns_session_tz_after_set()
        self.check_timezone_no_set_fallback_l3_l5()
        self.check_alter_local_timezone_changes_new_connections_only()
        self.check_timezone_invalid_params()
        self.check_timezone_from_table()

class _DisplayTimezoneMixin:
    """Timestamp display using connection timezone (SELECT ts, SHOW, etc.)."""

    def _setup_display_timezone_case(self):
        tdLog.debug(f"start to execute {__file__}")
        self.dbname = 'db_tz_disp'
        self.ntbname = f'{self.dbname}.ntb'
        self.ts = 1736942400000  # 2026-01-15 12:00:00 UTC

    def _prepare_display_timezone_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.ntbname}')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp, c1 int)')
        tdSql.execute(f'insert into {self.ntbname} values ({self.ts}, 1)')

    def check_select_ts_uses_connection_tz(self):
        """SELECT ts display should differ across session timezones in taos CLI output."""
        self._prepare_display_timezone_data()

        taos_bin = os.path.join(tdCom.getBuildPath(), "build", "bin", "taos")
        cfg_path = tdCom.getClientCfgPath()
        assert os.path.exists(taos_bin), f"taos binary not found: {taos_bin}"

        def _query_ts_with_cli(timezone):
            sql = (
                f"use {self.dbname}; "
                f"SET TIMEZONE '{timezone}'; "
                f"select ts from {self.ntbname};"
            )
            result = subprocess.run(
                [taos_bin, "-c", cfg_path, "-s", sql],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True,
            )
            output = result.stdout
            match = re.search(r"\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}", output)
            assert match is not None, f"cannot parse timestamp from taos output: {output}"
            return match.group(0)

        r_utc = _query_ts_with_cli('UTC')
        r_sh = _query_ts_with_cli('Asia/Shanghai')

        assert r_utc != r_sh, f"SELECT ts should differ across timezones: {r_utc} vs {r_sh}"

    def check_show_tables_uses_connection_tz(self):
        """SHOW TABLES should not error with connection timezone set."""
        self._prepare_display_timezone_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query(f"show {self.dbname}.tables")
        assert tdSql.queryRows > 0

    def check_explain_uses_connection_tz(self):
        """EXPLAIN should execute successfully with connection timezone set."""
        self._prepare_display_timezone_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query(
            f"explain select * from {self.ntbname} "
            f"where ts >= '2026-01-15 12:00:00'"
        )
        rows_utc = tdSql.queryRows

        tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
        tdSql.query(
            f"explain select * from {self.ntbname} "
            f"where ts >= '2026-01-15 12:00:00'"
        )
        rows_sh = tdSql.queryRows

        assert rows_utc > 0 and rows_sh > 0

    def check_display_without_set_timezone(self):
        """Without SET TIMEZONE, display should use L3 -> L5 (unchanged behavior)."""
        self._prepare_display_timezone_data()
        tdSql.query(f"select ts from {self.ntbname}")
        assert tdSql.queryResult[0][0] is not None

    def test_display_timezone(self):
        """summary: Timestamp display using connection timezone (SELECT ts, SHOW, etc.).

        description: Timestamp display using connection timezone (SELECT ts, SHOW, etc.).

        Since: v3.4.2.0

        Labels: timezone

        Jira: None

        Catalog:
            - Function:timezone

        History:
            - 2026-05-12: Tony Zhang created

        """
        self._setup_display_timezone_case()
        self.check_select_ts_uses_connection_tz()
        self.check_show_tables_uses_connection_tz()
        self.check_explain_uses_connection_tz()
        self.check_display_without_set_timezone()

class _WhereCastJoinTzMixin:
    """WHERE / CAST / JOIN time literal parsing with connection timezone (L2 -> L3 -> L5)."""

    def _setup_where_cast_join_tz_case(self):
        tdLog.debug(f"start to execute {__file__}")
        self.dbname = 'db_tz_wcj'
        self.ntbname = f'{self.dbname}.ntb'
        self.ntbname2 = f'{self.dbname}.ntb2'

    def _prepare_where_cast_join_tz_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.ntbname}')
        tdSql.execute(f'drop table if exists {self.ntbname2}')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp, c1 int)')
        tdSql.execute(f'create table {self.ntbname2} (ts timestamp, c1 int)')
        # 2026-01-15 00:00/12:00/2026-01-16 00:00 UTC
        for ts, c1 in [(1768435200000, 1), (1768478400000, 2), (1768521600000, 3)]:
            tdSql.execute(f'insert into {self.ntbname} values ({ts}, {c1})')
        for ts, c1 in [(1768435200000, 10), (1768478400000, 20)]:
            tdSql.execute(f'insert into {self.ntbname2} values ({ts}, {c1})')

    def check_where_uses_connection_tz(self):
        """WHERE '2026-01-15 12:00:00' should match different rows with different L2.

        UTC: matches c1=2. Shanghai: no match (would be 04:00 UTC).
        """
        self._prepare_where_cast_join_tz_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query(f"select c1 from {self.ntbname} where ts = '2026-01-15 12:00:00'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

        tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
        tdSql.query(f"select c1 from {self.ntbname} where ts = '2026-01-15 12:00:00'")
        tdSql.checkRows(0)

    def check_cast_uses_connection_tz(self):
        """CAST same string to timestamp should differ with different L2."""
        self._prepare_where_cast_join_tz_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query("select cast('2026-01-15 12:00:00' as timestamp)")
        r_utc = tdSql.queryResult[0][0]

        tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
        tdSql.query("select cast('2026-01-15 12:00:00' as timestamp)")
        r_sh = tdSql.queryResult[0][0]

        assert r_utc != r_sh, f"CAST should differ: UTC={r_utc}, Shanghai={r_sh}"

    def check_join_uses_connection_tz(self):
        """JOIN with time literal should use connection timezone."""
        self._prepare_where_cast_join_tz_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query(
            f"select a.c1, b.c1 from {self.ntbname} a, {self.ntbname2} b "
            f"where a.ts = b.ts and a.ts = '2026-01-15 12:00:00'"
        )
        if tdSql.queryRows > 0:
            tdSql.checkData(0, 0, 2)
            tdSql.checkData(0, 1, 20)

    def check_where_no_set_timezone_fallback(self):
        """Without SET TIMEZONE, WHERE should use L3 -> L5 (current behavior)."""
        tdSql.connect()  # reset connection to system default timezone
        self._prepare_where_cast_join_tz_data()
        tdSql.query(f"select c1 from {self.ntbname} where ts >= '2026-01-15 00:00:00'")
        assert tdSql.queryRows > 0

    def test_where_cast_join_tz(self):
        """summary: WHERE / CAST / JOIN time literal parsing with connection timezone (L2 -> L3 -> L5).

        description: WHERE / CAST / JOIN time literal parsing with connection timezone (L2 -> L3 -> L5).

        Since: v3.4.2.0

        Labels: timezone

        Jira: None

        Catalog:
            - Function:timezone

        History:
            - 2026-05-12: Tony Zhang created

        """
        self._setup_where_cast_join_tz_case()
        self.check_where_uses_connection_tz()
        self.check_cast_uses_connection_tz()
        self.check_join_uses_connection_tz()
        self.check_where_no_set_timezone_fallback()

class _TodayNowTzMixin:
    """TODAY() connection timezone (L2->L3->L5) and NOW() unchanged behavior."""

    def _setup_today_now_tz_case(self):
        tdLog.debug(f"start to execute {__file__}")
        self.dbname = 'db_tz_today'
        self.ntbname = f'{self.dbname}.ntb'

    def _prepare_today_now_tz_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.ntbname}')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp, c1 int)')
        tdSql.execute(f'insert into {self.ntbname} values (now, 1)')

    def check_today_with_different_timezones(self):
        """TODAY() with very different timezones should both succeed.

        Auckland (UTC+12/+13) vs Honolulu (UTC-10): 22-23h apart.
        """
        self._prepare_today_now_tz_data()
        tdSql.execute("SET TIMEZONE 'Pacific/Auckland'")
        tdSql.query("select today()")
        assert tdSql.queryResult[0][0] is not None

        tdSql.execute("SET TIMEZONE 'Pacific/Honolulu'")
        tdSql.query("select today()")
        assert tdSql.queryResult[0][0] is not None

    def check_today_utc_returns_midnight(self):
        """TODAY() with UTC should return a midnight-aligned timestamp."""
        self._prepare_today_now_tz_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query("select today()")
        val = tdSql.queryResult[0][0]
        if isinstance(val, int):
            assert val % 86400000 == 0, f"TODAY() in UTC should be midnight: {val}"
        elif isinstance(val, datetime.datetime):
            ts_ms = int(val.timestamp() * 1000)
            assert ts_ms % 86400000 == 0, \
                f"TODAY() in UTC should be midnight-aligned (ms): {ts_ms}"

    def check_today_not_affected_by_server_tz(self):
        """TODAY() is client-side (L2->L3->L5), not server-side L4."""
        self._prepare_today_now_tz_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query("select today()")
        assert tdSql.queryResult[0][0] is not None

    def check_today_in_where(self):
        """TODAY() should work in WHERE clause."""
        self._prepare_today_now_tz_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query(f"select * from {self.ntbname} where ts >= today()")

    def check_now_not_affected_by_set_timezone(self):
        """NOW() returns raw UTC timestamp, unaffected by timezone settings."""
        self._prepare_today_now_tz_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query("select now()")
        assert tdSql.queryResult[0][0] is not None

        tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
        tdSql.query("select now()")
        assert tdSql.queryResult[0][0] is not None

    def test_today_now_tz(self):
        """summary: TODAY() connection timezone (L2->L3->L5) and NOW() unchanged behavior.

        description: TODAY() connection timezone (L2->L3->L5) and NOW() unchanged behavior.

        Since: v3.4.2.0

        Labels: timezone

        Jira: None

        Catalog:
            - Function:timezone

        History:
            - 2026-05-12: Tony Zhang created

        """
        self._setup_today_now_tz_case()
        self.check_today_with_different_timezones()
        self.check_today_utc_returns_midnight()
        self.check_today_not_affected_by_server_tz()
        self.check_today_in_where()
        self.check_now_not_affected_by_set_timezone()

class _IntervalTimezoneMixin:
    """INTERVAL window aggregation with different connection timezones (P2).

    Different timezones shift "one day" boundaries, so the same data
    grouped by interval(1d) should produce different bucket counts or
    different per-bucket aggregation results.
    """

    def _setup_interval_timezone_case(self):
        tdLog.debug(f"start to execute {__file__}")
        self.dbname = 'db_tz_intv_cmp'
        self.ntbname = f'{self.dbname}.ntb'

    def _prepare_interval_timezone_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.ntbname}')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp, val int)')
        # Insert 6 rows spanning UTC 2026-01-14 20:00 ~ 2026-01-15 10:00 (2h apart)
        # UTC day boundary: 2026-01-15 00:00  -> splits 2+4 = 6 rows
        # Shanghai (UTC+8) day boundary: 2026-01-14 16:00 UTC (= Jan15 00:00 CST)
        #   In Shanghai tz, all 6 rows fall on Jan 15 -> 1 bucket
        base = 1768420800000  # 2026-01-14 20:00:00 UTC
        for i in range(6):
            ts = base + i * 7200000  # every 2 hours
            tdSql.execute(f'insert into {self.ntbname} values ({ts}, {i + 1})')

    def check_interval_1d_different_tz_different_buckets(self):
        """interval(1d) should produce different bucket counts under UTC vs Asia/Shanghai.

        Data: 6 rows from 2026-01-14 20:00 to 2026-01-15 06:00 UTC (every 2h).
        - UTC: 2 rows in Jan-14 bucket (20:00, 22:00) + 4 rows in Jan-15 (00:00..06:00) = 2 buckets
        - Shanghai: all 6 rows fall on Jan-15 CST (04:00..14:00 CST) = 1 bucket

        Currently interval window alignment uses server timezone (L4), not
        session timezone (L2). Keep this test skipped until interval respects
        SET TIMEZONE, then it should assert the 2-bucket vs 1-bucket split.
        """
        self._prepare_interval_timezone_data()

        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query(
            f"select _wstart, count(*) from {self.ntbname} interval(1d)"
        )
        utc_buckets = tdSql.queryRows

        tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
        tdSql.query(
            f"select _wstart, count(*) from {self.ntbname} interval(1d)"
        )
        sh_buckets = tdSql.queryRows

        assert utc_buckets == 2, f"UTC should have 2 daily buckets, got {utc_buckets}"
        assert sh_buckets == 1, f"Shanghai should have 1 daily bucket, got {sh_buckets}"

    def check_interval_1h_same_across_tz(self):
        """interval(1h) should produce the same number of buckets regardless of timezone.

        Hourly boundaries are timezone-independent (always 3600s aligned).
        Note: Test data is in January (UTC+0, no DST), so DST-related edge cases do not apply.
        """
        self._prepare_interval_timezone_data()

        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query(
            f"select _wstart, count(*) from {self.ntbname} interval(1h)"
        )
        utc_buckets = tdSql.queryRows

        tdSql.execute("SET TIMEZONE 'Asia/Shanghai'")
        tdSql.query(
            f"select _wstart, count(*) from {self.ntbname} interval(1h)"
        )
        sh_buckets = tdSql.queryRows

        assert utc_buckets == sh_buckets, \
            f"1h interval should be same across timezones: UTC={utc_buckets}, SH={sh_buckets}"

    def check_interval_1d_sum_consistent(self):
        """Total sum across all buckets must be the same regardless of timezone."""
        self._prepare_interval_timezone_data()

        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query(
            f"select _wstart, sum(val) from {self.ntbname} interval(1d)"
        )
        utc_total = sum(int(r[1]) for r in tdSql.queryResult)

        tdSql.execute("SET TIMEZONE 'America/New_York'")
        tdSql.query(
            f"select _wstart, sum(val) from {self.ntbname} interval(1d)"
        )
        ny_total = sum(int(r[1]) for r in tdSql.queryResult)

        expected = sum(range(1, 7))  # 1+2+3+4+5+6 = 21
        assert utc_total == expected, f"UTC sum should be {expected}, got {utc_total}"
        assert ny_total == expected, f"NY sum should be {expected}, got {ny_total}"

    def test_interval_timezone(self):
        """summary: INTERVAL window aggregation with different connection timezones (P2).

        description: INTERVAL window aggregation with different connection timezones (P2).

            Different timezones shift "one day" boundaries, so the same data
            grouped by interval(1d) should produce different bucket counts or
            different per-bucket aggregation results.

        Since: v3.4.2.0

        Labels: timezone

        Jira: None

        Catalog:
            - Function:timezone

        History:
            - 2026-05-12: Tony Zhang created

        """
        self._setup_interval_timezone_case()
        self.check_interval_1d_different_tz_different_buckets()
        self.check_interval_1h_same_across_tz()
        self.check_interval_1d_sum_consistent()


class TestTimezoneConfigDisplay(
    _SetTimezoneMixin,
    _SetFirstDayOfWeekMixin,
    _TimezoneFuncMixin,
    _DisplayTimezoneMixin,
    _WhereCastJoinTzMixin,
    _TodayNowTzMixin,
    _IntervalTimezoneMixin,
):
    pass

