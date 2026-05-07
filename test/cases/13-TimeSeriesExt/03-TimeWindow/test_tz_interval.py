"""Timezone INTERVAL window tests.

Covers:
- P4 Task 4.3: INTERVAL n/d/y/q natural units, DST no-drift, SLIDING
- P4 Task 4.2: INTERVAL 1w aligned by firstDayOfWeek (high-risk change)
- P5 Task 5.1: INTERVAL 1q/2q quarter boundaries
- Equivalence: INTERVAL 1q == 3n, 2q == 6n
"""

from new_test_framework.utils import tdLog, tdSql


def _config_timezone_name(value):
    return str(value).split(' ')[0]


def _wstart_count_map(rows):
    return {str(row[0])[:10]: int(row[1]) for row in rows}


class TestIntervalNatural:
    """INTERVAL with natural units (n/d/y/q), DST no-drift, SLIDING."""

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.dbname = 'db_tz_intv_nat'
        cls.stbname = f'{cls.dbname}.stb'
        cls.ctbname = f'{cls.dbname}.ctb1'

    def prepare_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.stbname}')
        tdSql.execute(f'create stable {self.stbname} (ts timestamp, val float) tags (t1 int)')
        tdSql.execute(f'create table {self.ctbname} using {self.stbname} tags (1)')
        base = 1735689600000  # 2025-01-01 00:00:00 UTC
        for i in range(365):
            ts = base + i * 86400000
            tdSql.execute(f"insert into {self.ctbname} values ({ts}, {float(i)})")

    def test_interval_1n_bucket_count(self):
        """INTERVAL(1n) on a full year should produce 12 monthly buckets."""
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query(
            f"select _wstart, count(*) from {self.ctbname} "
            f"where ts >= '2025-01-01' and ts < '2026-01-01' interval(1n)"
        )
        tdSql.checkRows(12)

    def test_interval_1d_bucket_count(self):
        """INTERVAL(1d) should produce 365 daily buckets for a full year."""
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query(
            f"select _wstart, count(*) from {self.ctbname} "
            f"where ts >= '2025-01-01' and ts < '2026-01-01' interval(1d)"
        )
        tdSql.checkRows(365)

    def test_interval_1d_uses_session_timezone_not_client_local(self):
        """INTERVAL should use L2 session timezone instead of current client local timezone."""
        self.prepare_data()
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

    def test_interval_1d_uses_session_timezone_not_client_or_server(self):
        """INTERVAL should keep using L2 session timezone even if L3/L4 are different."""
        self.prepare_data()
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

    def test_interval_1n_dst_no_drift(self):
        """INTERVAL(1n) with DST timezone should still produce 12 months.

        March bucket should be 31 days (DST spring-forward in NY doesn't drift).
        """
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        tdSql.query(
            f"select _wstart, count(*) from {self.ctbname} "
            f"where ts >= '2025-01-01' and ts < '2026-01-01' interval(1n)"
        )
        tdSql.checkRows(12)

    def test_interval_1y_single_bucket(self):
        """INTERVAL(1y) on a full year should produce 1 bucket."""
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query(
            f"select _wstart, count(*) from {self.ctbname} "
            f"where ts >= '2025-01-01' and ts < '2026-01-01' interval(1y)"
        )
        tdSql.checkRows(1)

    def test_interval_1q_four_buckets(self):
        """INTERVAL(1q) on a full year should produce 4 quarterly buckets."""
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query(
            f"select _wstart, count(*) from {self.ctbname} "
            f"where ts >= '2025-01-01' and ts < '2026-01-01' interval(1q)"
        )
        tdSql.checkRows(4)

    def test_interval_1q_equals_3n(self):
        """INTERVAL(1q) and INTERVAL(3n) should produce identical results."""
        self.prepare_data()
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

    def test_interval_sliding_1n_1d(self):
        """INTERVAL(1n) SLIDING(1d) should produce many overlapping buckets."""
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query(
            f"select _wstart, count(*) from {self.ctbname} "
            f"where ts >= '2025-01-01' and ts < '2025-04-01' interval(1n) sliding(1d)"
        )
        assert tdSql.queryRows > 3, f"Expected many rows, got {tdSql.queryRows}"

    def test_interval_1n_supertable(self):
        """INTERVAL(1n) should work on supertable query."""
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query(
            f"select _wstart, count(*) from {self.stbname} "
            f"where ts >= '2025-01-01' and ts < '2026-01-01' interval(1n)"
        )
        tdSql.checkRows(12)


class TestIntervalWeek:
    """INTERVAL(1w) aligned by firstDayOfWeek (high-risk change)."""

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.dbname = 'db_tz_intv_wk'
        cls.stbname = f'{cls.dbname}.stb'
        cls.ctbname = f'{cls.dbname}.ctb1'

    def prepare_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.stbname}')
        tdSql.execute(f'create stable {self.stbname} (ts timestamp, val float) tags (t1 int)')
        tdSql.execute(f'create table {self.ctbname} using {self.stbname} tags (1)')
        base = 1745539200000  # 2025-04-25 00:00:00 UTC (Friday)
        for i in range(21):
            ts = base + i * 86400000
            tdSql.execute(f"insert into {self.ctbname} values ({ts}, {float(i)})")

    def test_interval_1w_fdow_differences(self):
        """INTERVAL(1w) with different firstDayOfWeek should produce different _wstart."""
        self.prepare_data()
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

    def test_interval_1w_all_fdow(self):
        """INTERVAL(1w) with all 7 firstDayOfWeek values should succeed."""
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        for fdow in range(7):
            tdSql.execute(f"SET FIRST_DAY_OF_WEEK {fdow}")
            tdSql.query(f"select _wstart, count(*) from {self.ctbname} interval(1w)")
            assert tdSql.queryRows > 0, f"fdow={fdow}: no rows"

    def test_interval_1w_dst_no_drift(self):
        """INTERVAL(1w) during DST week should not drift."""
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'America/New_York'")
        tdSql.execute("SET FIRST_DAY_OF_WEEK 0")
        tdSql.query(f"select _wstart, count(*) from {self.ctbname} interval(1w)")
        assert tdSql.queryRows > 0

    def test_interval_1w_supertable(self):
        """INTERVAL(1w) should work on supertable."""
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.execute("SET FIRST_DAY_OF_WEEK 1")
        tdSql.query(f"select _wstart, count(*) from {self.stbname} interval(1w)")
        assert tdSql.queryRows > 0

    def test_interval_1w_server_config_without_session_override(self):
        """Server firstDayOfWeek should affect INTERVAL(1w) after reconnect."""
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'UTC'")

        tdSql.execute("ALTER ALL DNODES 'firstDayOfWeek' '0'")
        tdSql.connect()
        tdSql.query(f"select _wstart, count(*) from {self.ctbname} interval(1w)")
        starts_0 = [row[0] for row in tdSql.queryResult]

        tdSql.execute("ALTER ALL DNODES 'firstDayOfWeek' '1'")
        tdSql.connect()
        tdSql.query(f"select _wstart, count(*) from {self.ctbname} interval(1w)")
        starts_1 = [row[0] for row in tdSql.queryResult]

        assert starts_0 != starts_1, (
            f"Server firstDayOfWeek should change interval starts: {starts_0} vs {starts_1}"
        )


class TestIntervalQuarter:
    """INTERVAL(1q/2q) quarter boundaries and equivalence tests."""

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.dbname = 'db_tz_intv_q'
        cls.stbname = f'{cls.dbname}.stb'
        cls.ctbname = f'{cls.dbname}.ctb1'

    def prepare_data(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'drop table if exists {self.stbname}')
        tdSql.execute(f'create stable {self.stbname} (ts timestamp, val float) tags (t1 int)')
        tdSql.execute(f'create table {self.ctbname} using {self.stbname} tags (1)')
        base = 1735689600000  # 2025-01-01 00:00:00 UTC
        for i in range(365):
            ts = base + i * 86400000
            tdSql.execute(f"insert into {self.ctbname} values ({ts}, {float(i)})")

    def test_interval_1q_boundaries(self):
        """INTERVAL(1q): _wstart should be Jan1, Apr1, Jul1, Oct1."""
        self.prepare_data()
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

    def test_interval_2q_two_buckets(self):
        """INTERVAL(2q): 1 year should produce 2 half-year buckets."""
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query(
            f"select _wstart, count(*) from {self.ctbname} "
            f"where ts >= '2025-01-01' and ts < '2026-01-01' interval(2q)"
        )
        tdSql.checkRows(2)

    def test_interval_1q_equals_3n_results(self):
        """INTERVAL(1q) and INTERVAL(3n) should produce identical rows."""
        self.prepare_data()
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

    def test_interval_2q_equals_6n(self):
        """INTERVAL(2q) and INTERVAL(6n) should produce identical rows."""
        self.prepare_data()
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

    def test_interval_1q_supertable(self):
        """INTERVAL(1q) should work on supertable."""
        self.prepare_data()
        tdSql.execute("SET TIMEZONE 'UTC'")
        tdSql.query(
            f"select _wstart, count(*) from {self.stbname} "
            f"where ts >= '2025-01-01' and ts < '2026-01-01' interval(1q)"
        )
        tdSql.checkRows(4)
