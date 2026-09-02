"""Timezone FILL / INTERP daily-window tests.

These guard the regression fixed by passing the session timezone (instead of
NULL) into taosTimeAdd from the FILL operator.  With a DST session timezone,
advancing a daily window by one calendar day must yield 23h on the spring-forward
day and 25h on the fall-back day -- a NULL timezone would instead advance by a
fixed 24h and drift the generated window boundaries off local midnight.

All durations/deltas asserted here are computed by the *server* (via
`cast(_wend as bigint) - cast(_wstart as bigint)` for FILL and `diff(_irowts)`
for INTERP), not by the Python test, so they reflect exactly what the engine
produced.

Notes on INTERP:
- INTERP RANGE boundaries are parsed in the session timezone.
- INTERP EVERY(1d)/EVERY(1w) advance by *calendar* days/weeks (DST-aware), so the
  generated _irowts stay on local midnight across a DST transition (23h/25h step).
- INTERP EVERY(24h) keeps *fixed*-duration stepping: 1d and 24h are fundamentally
  different (a calendar day is 23h/25h across DST, 24h is always 24h).

Covers:
- FILL(NULL/PREV) INTERVAL(1d): filled window boundaries honor DST calendar days
- FILL boundaries match the (calendar-aware) INTERVAL operator boundaries
- INTERP RANGE boundary parsing follows the session timezone
- INTERP EVERY(1d) is DST-aware (calendar day); EVERY(24h) stays fixed
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

HOUR_MS = 3600000
DAY_MS = 86400000        # 24h
SPRING_DAY_MS = 82800000  # 23h (spring-forward calendar day)
FALL_DAY_MS = 90000000    # 25h (fall-back calendar day)

# America/New_York local-midnight anchors (epoch ms), reused from the INTERVAL suite.
NY_MAR_01_2025 = 1740805200000  # 2025-03-01 00:00:00 America/New_York
NY_NOV_01_2025 = 1761969600000  # 2025-11-01 00:00:00 America/New_York


class _FillDstMixin:
    """FILL(...) INTERVAL(1d) must advance window boundaries by calendar days."""

    def _setup_fill_dst_case(self):
        tdLog.debug(f"start to execute {__file__}")
        self.fill_dbname = 'db_tz_fill_dst'
        self.fill_ctbname = f'{self.fill_dbname}.ctb1'

    def _prepare_fill_sparse_data(self, points):
        """Create a fresh table holding only `points` (epoch ms) so that the
        intermediate daily windows must be produced by the FILL operator."""
        tdSql.execute(f'create database if not exists {self.fill_dbname}')
        tdSql.execute(f'use {self.fill_dbname}')
        tdSql.execute(f'drop table if exists {self.fill_dbname}.stb')
        tdSql.execute(f'create stable {self.fill_dbname}.stb (ts timestamp, val float) tags (t1 int)')
        tdSql.execute(f'create table {self.fill_ctbname} using {self.fill_dbname}.stb tags (1)')
        values = ','.join(f'({ts}, {float(i)})' for i, ts in enumerate(points))
        tdSql.execute(f'insert into {self.fill_ctbname} values {values}')

    def _query_fill_window_durations(self, start_local, end_local, fill_mode):
        """Return (durations_ms, wstarts, wends) for a daily FILL query.

        durations_ms is computed by the server as cast(_wend) - cast(_wstart).
        """
        tdSql.query(
            f"select cast(_wstart as bigint), cast(_wend as bigint), "
            f"cast(_wend as bigint) - cast(_wstart as bigint) "
            f"from {self.fill_ctbname} "
            f"where ts >= '{start_local}' and ts < '{end_local}' "
            f"interval(1d) fill({fill_mode})"
        )
        rows = list(tdSql.queryResult)
        wstarts = [int(r[0]) for r in rows]
        wends = [int(r[1]) for r in rows]
        durations = [int(r[2]) for r in rows]
        return durations, wstarts, wends

    def check_fill_null_1d_spring_forward_boundaries(self):
        """FILL(NULL) INTERVAL(1d) across spring-forward: middle day is 23h."""
        # Sparse data: one point in the Mar 8 window, one in Mar 11; Mar 9/10 filled.
        mar_08_noon = NY_MAR_01_2025 + 7 * DAY_MS + 12 * HOUR_MS
        mar_11_noon = NY_MAR_01_2025 + 8 * DAY_MS + SPRING_DAY_MS + DAY_MS + 12 * HOUR_MS
        self._prepare_fill_sparse_data([mar_08_noon, mar_11_noon])
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        durations, wstarts, wends = self._query_fill_window_durations(
            '2025-03-08 00:00:00', '2025-03-12 00:00:00', 'null')
        tdSql.checkRows(4)  # Mar 8, 9, 10, 11
        expected = [DAY_MS, SPRING_DAY_MS, DAY_MS, DAY_MS]  # Mar 9 is 23h
        assert durations == expected, (
            f"FILL(NULL) INTERVAL(1d) spring durations must follow DST calendar days: "
            f"expected {expected}, got {durations}"
        )
        # Windows must be contiguous: _wend[i] == _wstart[i+1] (server values).
        for i in range(len(wends) - 1):
            assert wends[i] == wstarts[i + 1], (
                f"FILL window not contiguous at row {i}: _wend={wends[i]} _wstart_next={wstarts[i + 1]}"
            )

    def check_fill_prev_1d_fall_back_boundaries(self):
        """FILL(PREV) INTERVAL(1d) across fall-back: middle day is 25h."""
        nov_01_noon = NY_NOV_01_2025 + 12 * HOUR_MS
        nov_04_noon = NY_NOV_01_2025 + DAY_MS + FALL_DAY_MS + DAY_MS + 12 * HOUR_MS
        self._prepare_fill_sparse_data([nov_01_noon, nov_04_noon])
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        durations, wstarts, wends = self._query_fill_window_durations(
            '2025-11-01 00:00:00', '2025-11-05 00:00:00', 'prev')
        tdSql.checkRows(4)  # Nov 1, 2, 3, 4
        expected = [DAY_MS, FALL_DAY_MS, DAY_MS, DAY_MS]  # Nov 2 is 25h
        assert durations == expected, (
            f"FILL(PREV) INTERVAL(1d) fall durations must follow DST calendar days: "
            f"expected {expected}, got {durations}"
        )
        for i in range(len(wends) - 1):
            assert wends[i] == wstarts[i + 1], (
                f"FILL window not contiguous at row {i}: _wend={wends[i]} _wstart_next={wstarts[i + 1]}"
            )

    def check_fill_1d_matches_unfilled_interval(self):
        """FILL boundaries must match the INTERVAL operator's session-tz boundaries.

        With one data point per day across the spring-forward week, the
        INTERVAL(1d) and INTERVAL(1d) FILL(NULL) queries must produce the exact
        same _wstart sequence -- a NULL-timezone FILL advance would drift after
        the 23h day and diverge from the (correct) interval boundaries.
        """
        day_starts = [
            NY_MAR_01_2025 + 7 * DAY_MS,                                   # Mar 8 00:00
            NY_MAR_01_2025 + 8 * DAY_MS,                                   # Mar 9 00:00
            NY_MAR_01_2025 + 8 * DAY_MS + SPRING_DAY_MS,                   # Mar 10 00:00
            NY_MAR_01_2025 + 8 * DAY_MS + SPRING_DAY_MS + DAY_MS,          # Mar 11 00:00
            NY_MAR_01_2025 + 8 * DAY_MS + SPRING_DAY_MS + 2 * DAY_MS,      # Mar 12 00:00
        ]
        points = [d + 12 * HOUR_MS for d in day_starts]
        self._prepare_fill_sparse_data(points)
        tdSql.execute("SET TIMEZONE 'America/New_York'")

        where = ("where ts >= '2025-03-08 00:00:00' and ts < '2025-03-13 00:00:00' "
                 "interval(1d)")
        tdSql.query(f"select cast(_wstart as bigint) from {self.fill_ctbname} {where}")
        interval_starts = [int(r[0]) for r in tdSql.queryResult]
        tdSql.query(f"select cast(_wstart as bigint) from {self.fill_ctbname} {where} fill(null)")
        fill_starts = [int(r[0]) for r in tdSql.queryResult]
        assert interval_starts == fill_starts, (
            f"FILL _wstart must match INTERVAL _wstart under session timezone: "
            f"interval={interval_starts}, fill={fill_starts}"
        )

    def test_fill_dst(self):
        """summary: FILL(...) INTERVAL(1d) honors session-timezone DST calendar days.

        description: FILL(NULL/PREV) over spring-forward / fall-back days must
            advance filled window boundaries by calendar days (23h / 25h) using
            the session timezone, not a fixed 24h.

        Since: v3.4.2.0

        Labels: timezone

        Jira: None

        Catalog:
            - Function:timezone

        History:
            - 2026-06-05: Tony Zhang created
        """
        self._setup_fill_dst_case()
        self.check_fill_null_1d_spring_forward_boundaries()
        self.check_fill_prev_1d_fall_back_boundaries()
        self.check_fill_1d_matches_unfilled_interval()


class _InterpDstMixin:
    """INTERP timezone behavior: RANGE parsing is session-tz aware; EVERY is fixed."""

    def _setup_interp_dst_case(self):
        tdLog.debug(f"start to execute {__file__}")
        self.interp_dbname = 'db_tz_interp_dst'
        self.interp_ctbname = f'{self.interp_dbname}.ctb1'

    def _prepare_interp_hourly_data(self, start_ms, end_ms):
        """Hourly data over [start_ms, end_ms) so INTERP has values to anchor on."""
        tdSql.execute(f'create database if not exists {self.interp_dbname}')
        tdSql.execute(f'use {self.interp_dbname}')
        tdSql.execute(f'drop table if exists {self.interp_dbname}.stb')
        tdSql.execute(f'create stable {self.interp_dbname}.stb (ts timestamp, val float) tags (t1 int)')
        tdSql.execute(f'create table {self.interp_ctbname} using {self.interp_dbname}.stb tags (1)')
        seq = 0
        values = []
        for ts in range(start_ms, end_ms, HOUR_MS):
            values.append(f'({ts}, {float(seq)})')
            seq += 1
            if len(values) >= 200:
                tdSql.execute(f"insert into {self.interp_ctbname} values {','.join(values)}")
                values = []
        if values:
            tdSql.execute(f"insert into {self.interp_ctbname} values {','.join(values)}")

    def _interp_first_irowts_ms(self, range_start_local, session_tz):
        """First _irowts (epoch ms) of an EVERY(1d) INTERP under `session_tz`."""
        tdSql.execute(f"SET TIMEZONE '{session_tz}'")
        tdSql.query(
            f"select cast(_irowts as bigint), interp(val) from {self.interp_ctbname} "
            f"range('{range_start_local}', '{range_start_local}') every(1d) fill(prev)"
        )
        assert tdSql.queryRows == 1, f"expected 1 interp point, got {tdSql.queryRows}"
        return int(tdSql.queryResult[0][0])

    def check_interp_range_honors_session_timezone(self):
        """INTERP RANGE boundaries are parsed in the session timezone.

        The same range string '2025-03-10 00:00:00' resolves to local midnight
        in each session timezone; New York (EDT, UTC-4) midnight is 4h after UTC
        midnight, so the generated _irowts must differ by exactly 4h.
        """
        # Hourly data Mar 7..Mar 13 ET so the interp point has a fill anchor.
        start = NY_MAR_01_2025 + 6 * DAY_MS
        end = NY_MAR_01_2025 + 8 * DAY_MS + SPRING_DAY_MS + 3 * DAY_MS
        self._prepare_interp_hourly_data(start, end)

        ms_ny = self._interp_first_irowts_ms('2025-03-10 00:00:00', 'America/New_York')
        ms_utc = self._interp_first_irowts_ms('2025-03-10 00:00:00', 'UTC')
        assert ms_ny - ms_utc == 4 * HOUR_MS, (
            f"INTERP RANGE must be parsed in the session timezone: "
            f"NY _irowts={ms_ny}, UTC _irowts={ms_utc}, diff={ms_ny - ms_utc} (expected {4 * HOUR_MS})"
        )

    def _query_interp_irowts_and_deltas(self, ctbname, start_local, end_local, every):
        """Return (irowts_ms, deltas_ms) for an EVERY(...) INTERP query.

        deltas_ms is computed by the server via diff(_irowts) so it reflects the
        engine's own step arithmetic, not Python.
        """
        tdSql.query(
            f"select cast(_irowts as bigint), interp(val) from {ctbname} "
            f"range('{start_local}', '{end_local}') every({every}) fill(prev)"
        )
        irowts = [int(r[0]) for r in tdSql.queryResult]
        tdSql.query(
            f"select diff(_irowts) from ("
            f"select _irowts, interp(val) v from {ctbname} "
            f"range('{start_local}', '{end_local}') every({every}) fill(prev))"
        )
        deltas = [int(r[0]) for r in tdSql.queryResult]
        return irowts, deltas

    def check_interp_every_1d_spring_forward(self):
        """INTERP EVERY(1d) is calendar/DST-aware: spring-forward day is 23h.

        The generated _irowts stay on local midnight (no drift), and the range
        end '2025-03-12 00:00:00' is included (5 points), unlike fixed 24h
        stepping which would drift to 01:00 and drop the final point.
        """
        start = NY_MAR_01_2025 + 6 * DAY_MS
        end = NY_MAR_01_2025 + 8 * DAY_MS + SPRING_DAY_MS + 3 * DAY_MS
        self._prepare_interp_hourly_data(start, end)
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        _, deltas = self._query_interp_irowts_and_deltas(
            self.interp_ctbname, '2025-03-08 00:00:00', '2025-03-12 00:00:00', '1d')
        assert deltas == [DAY_MS, SPRING_DAY_MS, DAY_MS, DAY_MS], (
            f"INTERP EVERY(1d) spring _irowts must follow DST calendar days "
            f"(24h,23h,24h,24h): got {deltas}"
        )

    def check_interp_every_1d_fall_back(self):
        """INTERP EVERY(1d) is calendar/DST-aware: fall-back day is 25h."""
        start = NY_NOV_01_2025 - DAY_MS
        end = NY_NOV_01_2025 + DAY_MS + FALL_DAY_MS + 4 * DAY_MS
        self._prepare_interp_hourly_data(start, end)
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        _, deltas = self._query_interp_irowts_and_deltas(
            self.interp_ctbname, '2025-11-01 00:00:00', '2025-11-05 00:00:00', '1d')
        assert deltas == [DAY_MS, FALL_DAY_MS, DAY_MS, DAY_MS], (
            f"INTERP EVERY(1d) fall _irowts must follow DST calendar days "
            f"(24h,25h,24h,24h): got {deltas}"
        )

    def check_interp_every_24h_stays_fixed(self):
        """INTERP EVERY(24h) keeps fixed-duration stepping (NOT calendar-aware).

        24h is fundamentally different from 1d: across the spring-forward day the
        _irowts drift off local midnight and every diff(_irowts) stays 24h.
        """
        start = NY_MAR_01_2025 + 6 * DAY_MS
        end = NY_MAR_01_2025 + 8 * DAY_MS + SPRING_DAY_MS + 3 * DAY_MS
        self._prepare_interp_hourly_data(start, end)
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        _, deltas = self._query_interp_irowts_and_deltas(
            self.interp_ctbname, '2025-03-08 00:00:00', '2025-03-12 00:00:00', '24h')
        assert deltas == [DAY_MS, DAY_MS, DAY_MS], (
            f"INTERP EVERY(24h) must stay fixed-duration (24h each): got {deltas}"
        )

    def test_interp_dst(self):
        """summary: INTERP timezone behavior across DST.

        description: INTERP RANGE boundaries follow the session timezone; EVERY(1d)
            advances by calendar days (DST-aware, 23h/25h), while EVERY(24h) stays
            fixed-duration.

        Since: v3.4.2.0

        Labels: timezone

        Jira: None

        Catalog:
            - Function:timezone

        History:
            - 2026-06-05: Tony Zhang created
        """
        self._setup_interp_dst_case()
        self.check_interp_range_honors_session_timezone()
        self.check_interp_every_1d_spring_forward()
        self.check_interp_every_1d_fall_back()
        self.check_interp_every_24h_stays_fixed()


class TestTimezoneFillInterp(
    _FillDstMixin,
    _InterpDstMixin,
):
    pass
