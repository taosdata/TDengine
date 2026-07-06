"""test_fq_04_timezone_precision — Timezone & Precision full-coverage test.

Tests timezone-aware vs timezone-naive interpretation, cross-precision alignment,
time functions, pseudo-columns, and edge cases across MySQL/PG/InfluxDB/local sources.

Catalog: - Query:FederatedTimezonePrecision

Since: v3.4.0.0

Labels: common,ci

History:
    - 2025-05-23 wpan  Initial creation

== BASELINE MAINTENANCE POLICY ==

  **NEVER** regenerate baseline files by copying tmp output.  The workflow of
  "delete baseline → run test → copy tmp to ans/" is STRICTLY PROHIBITED.

  Baseline files (under ans/) are the source of truth for expected results.
  They may only be updated by:
    1. Manually confirming the specific entry in the baseline file is WRONG.
    2. Manually editing ONLY the affected entry with the correct expected value.
    3. Reviewing the change to ensure no unintended modifications slip in.

  The tmp/ directory output is for debugging diff details ONLY.
"""

import datetime as _dt_mod
import os
import re
import time as _time

import pytz as _pytz
import taos.field as _taos_field

try:
    from zoneinfo import ZoneInfo as _ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo as _ZoneInfo

from new_test_framework.utils import tdLog, tdSql
from federated_query_common import (
    ExtSrcEnv,
    FederatedQueryCaseHelper,
    FederatedQueryTestMixin,
    _code,
)

# ── Error codes for negative cases ──
TSDB_CODE_PAR_WRONG_VALUE_TYPE = _code('TSDB_CODE_PAR_WRONG_VALUE_TYPE')
TSDB_CODE_INVALID_TIMESTAMP = _code('TSDB_CODE_INVALID_TIMESTAMP')
TSDB_CODE_FUNC_TIME_UNIT_TOO_SMALL = _code('TSDB_CODE_FUNC_TIME_UNIT_TOO_SMALL')

# =====================================================================
# Constants
# =====================================================================

# Naming
_MYSQL_DB   = "fq04_mdb"
_PG_DB      = "fq04_pdb"
_INFLUX_DB  = "fq04_idb"
_LOCAL_DB_MS = "fq04_local_ms"
_LOCAL_DB_US = "fq04_local_us"
_LOCAL_DB_NS = "fq04_local_ns"
_SRC_M = "fq04_src_m"
_SRC_P = "fq04_src_p"
_SRC_I = "fq04_src_i"

# ── Core 5-row data ──
# (ms_epoch, us_epoch, ns_epoch, val)
_ROWS = [
    (1704067200000, 1704067200000000, 1704067200000000000, 1),  # 00:00:00.000 aligned
    (1704067260000, 1704067260000000, 1704067260000000000, 2),  # 00:01:00.000 aligned
    (1704067320123, 1704067320123456, 1704067320123456789, 3),  # 00:02:00.123456789 diff
    (1704067380000, 1704067380000500, 1704067380000500500, 4),  # 00:03:00.000500500 diff
    (1704067440000, 1704067440000000, 1704067440000000000, 5),  # 00:04:00.000000000 aligned
]

# ── Common expected values derived from _ROWS for independent verification ──
# Tests using these catch precision bugs even if baseline file is wrong.
_E_TRUE = [[True]]
_E_COUNT5 = [[5]]
_E_COUNT0 = [[0]]
_E_VALS = [[r[3]] for r in _ROWS]  # [[1], [2], [3], [4], [5]]

# Sentinel — use in expect cells to skip matching for that column.
# Enables verifying deterministic columns while ignoring non-deterministic ones
# (e.g. NOW()/TODAY() value) in the same row.
class _AnyValue:
    """Matches any actual value in _check_expect."""
    _instance = None
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    def __repr__(self):
        return "<ANY>"
_ANY = _AnyValue()

# ── TZ-test timestamps (µs) ──
_TZ_TIMESTAMPS = [
    (1704067200000000, 1),   # 2024-01-01 00:00:00 UTC = 08:00:00 CST
    (1704139200000000, 2),   # 2024-01-01 20:00:00 UTC = 2024-01-02 04:00:00 CST (date cross!)
    (1704110400000000, 3),   # 2024-01-01 12:00:00 UTC = 20:00:00 CST
    (1718438400000000, 4),   # 2024-06-15 08:00:00 UTC = 16:00:00 CST (half year later)
]

# ── Multi-ts data (µs, 3 rows) ──
_MULTI_TS_DATA = [
    (1704067200000000, 1704096000000000, 1704067200000000, 1704067200000, 1),
    (1704067260000000, 1704096060000000, 1704067260000000, 1704067260000, 2),
    (1704067320000000, 1704096120000000, 1704067320000000, 1704067320000, 3),
]  # (ts_pk, ts_aware, ts_naive, ts_date_epoch, val)


def _ms_to_dt_str(ms):
    """Convert ms epoch to 'YYYY-MM-DD HH:MM:SS.mmm' in UTC.

    Uses explicit UTC so the calendar strings written to tz-naive columns
    (MySQL DATETIME, PG TIMESTAMP WITHOUT TZ) are deterministic regardless
    of the OS timezone of the container running the test.
    """
    dt = _dt_mod.datetime.fromtimestamp(ms / 1000.0, tz=_dt_mod.timezone.utc)
    return dt.strftime('%Y-%m-%d %H:%M:%S.') + f"{ms % 1000:03d}"


def _us_to_dt_str(us):
    """Convert µs epoch to 'YYYY-MM-DD HH:MM:SS.uuuuuu' in UTC.

    Uses explicit UTC so the calendar strings written to tz-naive columns
    are deterministic regardless of the OS timezone.
    """
    dt = _dt_mod.datetime.fromtimestamp(us / 1_000_000.0, tz=_dt_mod.timezone.utc)
    frac = us % 1_000_000
    return dt.strftime('%Y-%m-%d %H:%M:%S.') + f"{frac:06d}"


def _us_to_tz_local_str(us, tz_name):
    """Convert µs epoch to 'YYYY-MM-DD HH:MM:SS.uuuuuu' in the given IANA timezone.

    Used to write tz-unaware PG TIMESTAMP values that represent the same wall-clock
    moment as a given UTC epoch, so that a PG connector using clientTz=tz_name will
    reconstruct the original UTC epoch correctly.
    """
    dt_utc = _dt_mod.datetime(1970, 1, 1, tzinfo=_dt_mod.timezone.utc) + \
             _dt_mod.timedelta(microseconds=us)
    dt_local = dt_utc.astimezone(_ZoneInfo(tz_name))
    frac = us % 1_000_000
    return dt_local.strftime('%Y-%m-%d %H:%M:%S.') + f"{frac:06d}"


# =====================================================================
# MySQL data setup
# =====================================================================

_MYSQL_SETUP = [
    "DROP TABLE IF EXISTS `ts_types`",
    """CREATE TABLE `ts_types` (
        id INT AUTO_INCREMENT PRIMARY KEY,
        c_timestamp TIMESTAMP(6),
        c_datetime  DATETIME(6),
        c_datetime3 DATETIME(3),
        c_date      DATE
    ) ENGINE=InnoDB""",
    # ts_types data — tz test timestamps
    f"INSERT INTO `ts_types` (c_timestamp, c_datetime, c_datetime3, c_date) VALUES "
    f"(FROM_UNIXTIME({_TZ_TIMESTAMPS[0][0] / 1_000_000}), '2024-01-01 08:00:00', '2024-01-01 08:00:00', '2024-01-01'),"
    f"(FROM_UNIXTIME({_TZ_TIMESTAMPS[1][0] / 1_000_000}), '2024-01-02 04:00:00', '2024-01-02 04:00:00', '2024-01-02'),"
    f"(FROM_UNIXTIME({_TZ_TIMESTAMPS[2][0] / 1_000_000}), '2024-01-01 20:00:00', '2024-01-01 20:00:00', '2024-06-15'),"
    f"(FROM_UNIXTIME({_TZ_TIMESTAMPS[3][0] / 1_000_000}), '2024-06-15 16:00:00', '2024-06-15 16:00:00', '2024-06-15')",

    "DROP TABLE IF EXISTS `pk_dt`",
    """CREATE TABLE `pk_dt` (
        ts DATETIME(3) PRIMARY KEY,
        val INT
    ) ENGINE=InnoDB""",

    "DROP TABLE IF EXISTS `pk_ts`",
    """CREATE TABLE `pk_ts` (
        ts TIMESTAMP(6) PRIMARY KEY,
        val INT
    ) ENGINE=InnoDB""",

    "DROP TABLE IF EXISTS `multi_ts`",
    """CREATE TABLE `multi_ts` (
        ts DATETIME(3) PRIMARY KEY,
        ts_aware TIMESTAMP(6),
        ts_naive DATETIME(6),
        ts_date  DATE,
        val INT
    ) ENGINE=InnoDB""",
]

# Insert 5 core rows into pk_dt/pk_ts using ms-aligned datetimes
for _ms, _us, _ns, _v in _ROWS:
    _dt = _ms_to_dt_str(_ms)
    _MYSQL_SETUP.append(f"INSERT INTO `pk_dt` VALUES ('{_dt}', {_v})")
    _MYSQL_SETUP.append(f"INSERT INTO `pk_ts` VALUES (FROM_UNIXTIME({_us / 1_000_000}), {_v})")

# Insert multi_ts data
for _ts_pk, _ts_aware, _ts_naive, _ts_date_epoch, _v in _MULTI_TS_DATA:
    _pk_dt = _us_to_dt_str(_ts_pk)[:23]  # DATETIME(3) — ms precision
    _aware_str = f"FROM_UNIXTIME({_ts_aware / 1_000_000})"
    _naive_str = _us_to_dt_str(_ts_naive)
    _date_str = _dt_mod.datetime.utcfromtimestamp(_ts_date_epoch / 1000.0).strftime('%Y-%m-%d')
    _MYSQL_SETUP.append(
        f"INSERT INTO `multi_ts` VALUES ('{_pk_dt}', {_aware_str}, '{_naive_str}', '{_date_str}', {_v})"
    )

# =====================================================================
# PostgreSQL data setup
# =====================================================================

_PG_SETUP = [
    "DROP TABLE IF EXISTS ts_types",
    """CREATE TABLE ts_types (
        id SERIAL PRIMARY KEY,
        c_timestamptz TIMESTAMPTZ,
        c_timestamp   TIMESTAMP,
        c_date        DATE
    )""",
]

# ts_types data — tz test timestamps
for _us, _v in _TZ_TIMESTAMPS:
    _PG_SETUP.append(
        f"INSERT INTO ts_types (c_timestamptz, c_timestamp, c_date) VALUES "
        f"(to_timestamp({_us / 1_000_000}), '{_us_to_dt_str(_us)}', "
        f"'{_dt_mod.datetime.utcfromtimestamp(_us / 1_000_000).strftime('%Y-%m-%d')}')"
    )

_PG_SETUP += [
    "DROP TABLE IF EXISTS pk_ts",
    """CREATE TABLE pk_ts (
        ts TIMESTAMP PRIMARY KEY,
        val INT
    )""",

    "DROP TABLE IF EXISTS pk_tstz",
    """CREATE TABLE pk_tstz (
        ts TIMESTAMPTZ PRIMARY KEY,
        val INT
    )""",

    # pk_ts_parity: tz-unaware parity table for the CST session in test_timezone.
    # Populated with CST (Asia/Shanghai) local-time strings so that
    # mktime_z(CST, CST_string) reconstructs the correct UTC epoch in the CST phase.
    # The UTC session uses pk_ts (UTC strings) instead — see test_timezone loop.
    "DROP TABLE IF EXISTS pk_ts_parity",
    """CREATE TABLE pk_ts_parity (
        ts TIMESTAMP PRIMARY KEY,
        val INT
    )""",

    "DROP TABLE IF EXISTS multi_ts",
    """CREATE TABLE multi_ts (
        ts TIMESTAMP PRIMARY KEY,
        ts_aware TIMESTAMPTZ,
        ts_naive TIMESTAMP,
        ts_date  DATE,
        val INT
    )""",
]

for _ms, _us, _ns, _v in _ROWS:
    _dt_str = _us_to_dt_str(_us)
    _cst_str = _us_to_tz_local_str(_us, "Asia/Shanghai")
    _PG_SETUP.append(f"INSERT INTO pk_ts VALUES ('{_dt_str}', {_v})")
    _PG_SETUP.append(f"INSERT INTO pk_tstz VALUES (to_timestamp({_us / 1_000_000}), {_v})")
    _PG_SETUP.append(f"INSERT INTO pk_ts_parity VALUES ('{_cst_str}', {_v})")

for _ts_pk, _ts_aware, _ts_naive, _ts_date_epoch, _v in _MULTI_TS_DATA:
    _pk_str = _us_to_dt_str(_ts_pk)
    _date_str = _dt_mod.datetime.utcfromtimestamp(_ts_date_epoch / 1000.0).strftime('%Y-%m-%d')
    _PG_SETUP.append(
        f"INSERT INTO multi_ts VALUES ('{_pk_str}', to_timestamp({_ts_aware / 1_000_000}), "
        f"'{_us_to_dt_str(_ts_naive)}', '{_date_str}', {_v})"
    )

# =====================================================================
# InfluxDB data setup (line protocol, ns precision)
# =====================================================================

_INFLUX_LINES = []
for _ms, _us, _ns, _v in _ROWS:
    _INFLUX_LINES.append(f"sensor val={_v}i {_ns}")

# =====================================================================
# Local TDengine setup
# =====================================================================

_LOCAL_SETUP = []
for _prec, _db in [("ms", _LOCAL_DB_MS), ("us", _LOCAL_DB_US), ("ns", _LOCAL_DB_NS)]:
    _LOCAL_SETUP += [
        f"DROP DATABASE IF EXISTS {_db}",
        f"CREATE DATABASE {_db} PRECISION '{_prec}'",
        f"CREATE TABLE {_db}.t (ts TIMESTAMP, val INT)",
    ]
    for _ms, _us, _ns, _v in _ROWS:
        if _prec == "ms":
            _LOCAL_SETUP.append(f"INSERT INTO {_db}.t VALUES ({_ms}, {_v})")
        elif _prec == "us":
            _LOCAL_SETUP.append(f"INSERT INTO {_db}.t VALUES ({_us}, {_v})")
        else:
            _LOCAL_SETUP.append(f"INSERT INTO {_db}.t VALUES ({_ns}, {_v})")

# Insert target tables for ins group
for _prec, _db in [("ms", _LOCAL_DB_MS), ("us", _LOCAL_DB_US), ("ns", _LOCAL_DB_NS)]:
    _LOCAL_SETUP.append(f"CREATE TABLE {_db}.ins_target (ts TIMESTAMP, val INT)")


# =====================================================================
# Test case definitions
# =====================================================================

# ── TZ group (two-phase: CST then UTC) ──
_TZ_CASES = [
    # MySQL
    ("tz-m01", "SELECT CAST(c_timestamp AS BIGINT) FROM {M}.ts_types ORDER BY id"),
    ("tz-m02", "SELECT CAST(c_datetime AS BIGINT) FROM {M}.ts_types ORDER BY id"),
    ("tz-m03", "SELECT CAST(c_datetime3 AS BIGINT) FROM {M}.ts_types ORDER BY id"),
    ("tz-m04", "SELECT CAST(c_date AS BIGINT) FROM {M}.ts_types ORDER BY id"),
    ("tz-m05", "SELECT TO_ISO8601(c_timestamp) FROM {M}.ts_types ORDER BY id"),
    ("tz-m06", "SELECT TO_ISO8601(c_datetime) FROM {M}.ts_types ORDER BY id"),
    ("tz-m07", "SELECT val FROM {M}.pk_ts WHERE ts >= '2024-01-01' AND ts < '2024-01-02' ORDER BY ts"),
    ("tz-m08", "SELECT val FROM {M}.pk_ts WHERE ts >= 1704067200000 AND ts < 1704153600000 ORDER BY ts"),
    ("tz-m09", "SELECT val FROM {M}.pk_dt WHERE ts >= '2024-01-01' AND ts < '2024-01-02' ORDER BY ts"),
    ("tz-m10", "SELECT val FROM {M}.pk_dt WHERE ts >= 1704067200000 AND ts < 1704153600000 ORDER BY ts"),
    ("tz-m11", "SELECT WEEKDAY(c_datetime) FROM {M}.ts_types ORDER BY id"),
    ("tz-m12", "SELECT DATE(c_datetime) FROM {M}.ts_types ORDER BY id"),
    # PG
    ("tz-p01", "SELECT CAST(c_timestamptz AS BIGINT) FROM {P}.ts_types ORDER BY id"),
    ("tz-p02", "SELECT CAST(c_timestamp AS BIGINT) FROM {P}.ts_types ORDER BY id"),
    ("tz-p03", "SELECT CAST(c_date AS BIGINT) FROM {P}.ts_types ORDER BY id"),
    ("tz-p04", "SELECT TO_ISO8601(c_timestamptz) FROM {P}.ts_types ORDER BY id"),
    ("tz-p05", "SELECT TO_ISO8601(c_timestamp) FROM {P}.ts_types ORDER BY id"),
    ("tz-p06", "SELECT val FROM {P}.pk_tstz WHERE ts >= '2024-01-01' AND ts < '2024-01-02' ORDER BY ts"),
    ("tz-p07", "SELECT val FROM {P}.pk_tstz WHERE ts >= 1704067200000 AND ts < 1704153600000 ORDER BY ts"),
    ("tz-p08", "SELECT val FROM {P}.pk_ts WHERE ts >= '2024-01-01' AND ts < '2024-01-02' ORDER BY ts"),
    ("tz-p09", "SELECT val FROM {P}.pk_ts WHERE ts >= 1704067200000 AND ts < 1704153600000 ORDER BY ts"),
    ("tz-p10", "SELECT WEEKDAY(c_timestamp) FROM {P}.ts_types ORDER BY id"),
    ("tz-p11", "SELECT DATE(c_timestamptz) FROM {P}.ts_types ORDER BY id"),
    # InfluxDB
    ("tz-i01", "SELECT CAST(time AS BIGINT) FROM {I}.sensor ORDER BY time"),
    ("tz-i02", "SELECT TO_ISO8601(time) FROM {I}.sensor ORDER BY time"),
    ("tz-i03", "SELECT WEEKDAY(time) FROM {I}.sensor ORDER BY time"),
    # Cross-source
    ("tz-x01", "SELECT TIMEZONE() FROM {M}.pk_dt LIMIT 1"),
    ("tz-x02", "SELECT a.val FROM {M}.pk_ts a INNER JOIN {P}.pk_tstz b ON a.ts = b.ts ORDER BY a.ts"),
    ("tz-x03", "SELECT a.val FROM {M}.pk_dt a INNER JOIN {P}.pk_ts b ON a.ts = b.ts ORDER BY a.ts"),
    # ── tz-lf: time-function timezone behavior on local TDengine table ──
    # These functions produce timezone-sensitive output; testing them here
    # under both CST and UTC is the correct design rather than hardcoding
    # a timezone assumption inside test_time_functions.
    ("tz-lf01", "SELECT TO_ISO8601(ts) FROM {L_us}.t ORDER BY ts"),
    ("tz-lf02", "SELECT TO_CHAR(ts, '%Y/%m/%d %H:%M:%S') FROM {L_us}.t ORDER BY ts"),
    ("tz-lf03", "SELECT DATE(ts) FROM {L_us}.t ORDER BY ts"),
    ("tz-lf04", "SELECT TIMEZONE() FROM {L_us}.t LIMIT 1"),
    ("tz-lf05", "SELECT TIMETRUNCATE(ts, 1d) FROM {L_us}.t ORDER BY ts"),
    ("tz-lf06", "SELECT TO_ISO8601(FIRST(ts)) FROM {L_us}.t GROUP BY val ORDER BY val"),
    ("tz-lf07", "SELECT TO_CHAR(ts, '%H:%M:%S') FROM {L_us}.t ORDER BY ts"),
    ("tz-lf08", "SELECT TO_ISO8601(ts) FROM (SELECT ts FROM {L_us}.t ORDER BY ts)"),
    # TZ-sensitive edge semantics: these outputs depend on session timezone and
    # therefore must be covered in the timezone group rather than edge group.
    ("neg-02", "SELECT CAST(TO_TIMESTAMP('2024-01-01', '%H:%M:%S') AS BIGINT) FROM {M}.pk_dt LIMIT 1"),
    ("neg-03", "SELECT TO_TIMESTAMP('2024-13-01', '%Y-%m-%d') FROM {M}.pk_dt LIMIT 1"),
    ("neg-04", "SELECT CAST(TO_TIMESTAMP('2024-01-01', '%Q') AS BIGINT) FROM {M}.pk_dt LIMIT 1"),
]

# ── TFMT group (WHERE time format variants — TZ-insensitive only) ──
# TZ-sensitive cases moved to _TFMT_TZ_CASES.
_TFMT_CASES = [
    ("tfmt-02", "SELECT val FROM {M}.pk_dt WHERE ts > 1704067260000 ORDER BY ts"),
    ("tfmt-04", "SELECT val FROM {M}.pk_ts WHERE ts > 1704067260000 ORDER BY ts"),
    ("tfmt-06", "SELECT val FROM {M}.pk_dt WHERE ts BETWEEN 1704067260000 AND 1704067380000 ORDER BY ts"),
    ("tfmt-08", "SELECT val FROM {P}.pk_ts WHERE ts > 1704067260000 ORDER BY ts"),
    ("tfmt-10", "SELECT val FROM {P}.pk_tstz WHERE ts > 1704067260000 ORDER BY ts"),
    ("tfmt-12", "SELECT val FROM {I}.sensor WHERE time > 1704067260000 ORDER BY time"),
    ("tfmt-13", "SELECT val FROM {I}.sensor WHERE time > 1704067260000000 ORDER BY time"),
    ("tfmt-14", "SELECT val FROM {I}.sensor WHERE time > 1704067260000000000 ORDER BY time"),
    ("tfmt-16", "SELECT val FROM {L_ms}.t WHERE ts > 1704067260000 ORDER BY ts"),
    ("tfmt-17", "SELECT val FROM {L_us}.t WHERE ts > 1704067260000000 ORDER BY ts"),
    ("tfmt-18", "SELECT val FROM {L_ns}.t WHERE ts > 1704067260000000000 ORDER BY ts"),
    ("tfmt-19", "SELECT val FROM {M}.pk_dt WHERE ts < NOW() ORDER BY ts"),
    ("tfmt-20", "SELECT val FROM {M}.pk_dt WHERE ts < TODAY() ORDER BY ts"),
]

# ── TFMT-TZ group (TZ-sensitive WHERE time format variants; tested in test_timezone) ──
# These produce different results under CST vs UTC because:
#   tfmt-03/tfmt-09: µs epoch pushed down as calendar string using clientTz.
#   tfmt-11: calendar string interpreted with clientTz by InfluxDB connector.
_TFMT_TZ_CASES = [
    ("tfmt-01", "SELECT val FROM {M}.pk_dt WHERE ts > '2024-01-01 00:01:00' ORDER BY ts"),
    ("tfmt-03", "SELECT val FROM {M}.pk_dt WHERE ts > 1704067260000000 ORDER BY ts"),
    ("tfmt-05", "SELECT val FROM {M}.pk_dt WHERE ts BETWEEN '2024-01-01 00:01:00' AND '2024-01-01 00:03:00' ORDER BY ts"),
    ("tfmt-07", "SELECT val FROM {P}.pk_ts WHERE ts > '2024-01-01 00:01:00' ORDER BY ts"),
    ("tfmt-09", "SELECT val FROM {P}.pk_ts WHERE ts > 1704067260000000 ORDER BY ts"),
    ("tfmt-11", "SELECT val FROM {I}.sensor WHERE time > '2024-01-01 00:01:00' ORDER BY time"),
    ("tfmt-15", "SELECT val FROM {M}.pk_dt WHERE ts > 1704067260000 UNION ALL SELECT val FROM {P}.pk_ts WHERE ts > '2024-01-01 00:01:00' ORDER BY 1"),
]

# ── PREC group (single precision correctness — TZ-insensitive only) ──
# TZ-sensitive cases (prec-01,02,03,09–13) moved to _PREC_TZ_CASES.
_PREC_CASES = [
    ("prec-04", "SELECT CAST(ts AS BIGINT), val FROM {P}.pk_tstz ORDER BY ts"),
    ("prec-05", "SELECT CAST(time AS BIGINT), val FROM {I}.sensor ORDER BY time"),
    ("prec-06", "SELECT CAST(ts AS BIGINT), val FROM {L_ms}.t ORDER BY ts",
        {"expect": [[r[0], r[3]] for r in _ROWS]}),
    ("prec-07", "SELECT CAST(ts AS BIGINT), val FROM {L_us}.t ORDER BY ts",
        {"expect": [[r[1], r[3]] for r in _ROWS]}),
    ("prec-08", "SELECT CAST(ts AS BIGINT), val FROM {L_ns}.t ORDER BY ts",
        {"expect": [[r[2], r[3]] for r in _ROWS]}),
    # ── TIMETRUNCATE — verify truncation preserves source precision ──
    ("prec-14", "SELECT CAST(TIMETRUNCATE(ts, 1s) AS BIGINT) FROM {P}.pk_tstz ORDER BY ts"),
    ("prec-15", "SELECT CAST(TIMETRUNCATE(time, 1s) AS BIGINT) FROM {I}.sensor ORDER BY time"),
    ("prec-16", "SELECT CAST(TIMETRUNCATE(ts, 1s) AS BIGINT) FROM {L_ms}.t ORDER BY ts",
        {"expect": [[r[0] // 1000 * 1000] for r in _ROWS]}),
    ("prec-17", "SELECT CAST(TIMETRUNCATE(ts, 1s) AS BIGINT) FROM {L_us}.t ORDER BY ts",
        {"expect": [[r[1] // 1_000_000 * 1_000_000] for r in _ROWS]}),
    ("prec-18", "SELECT CAST(TIMETRUNCATE(ts, 1s) AS BIGINT) FROM {L_ns}.t ORDER BY ts",
        {"expect": [[r[2] // 1_000_000_000 * 1_000_000_000] for r in _ROWS]}),
    # ── MIN/MAX — aggregate preserves precision ──
    ("prec-19", "SELECT CAST(MIN(ts) AS BIGINT), CAST(MAX(ts) AS BIGINT) FROM {P}.pk_tstz"),
    ("prec-20", "SELECT CAST(MIN(time) AS BIGINT), CAST(MAX(time) AS BIGINT) FROM {I}.sensor"),
    ("prec-21", "SELECT CAST(MIN(ts) AS BIGINT), CAST(MAX(ts) AS BIGINT) FROM {L_ms}.t",
        {"expect": [[_ROWS[0][0], _ROWS[-1][0]]]}),
    ("prec-22", "SELECT CAST(MIN(ts) AS BIGINT), CAST(MAX(ts) AS BIGINT) FROM {L_us}.t",
        {"expect": [[_ROWS[0][1], _ROWS[-1][1]]]}),
    ("prec-23", "SELECT CAST(MIN(ts) AS BIGINT), CAST(MAX(ts) AS BIGINT) FROM {L_ns}.t",
        {"expect": [[_ROWS[0][2], _ROWS[-1][2]]]}),
    # ── FIRST/LAST — row-selection aggregate precision ──
    ("prec-24", "SELECT CAST(FIRST(time) AS BIGINT), CAST(LAST(time) AS BIGINT) FROM {I}.sensor"),
    ("prec-25", "SELECT CAST(FIRST(ts) AS BIGINT), CAST(LAST(ts) AS BIGINT) FROM {L_us}.t",
        {"expect": [[_ROWS[0][1], _ROWS[-1][1]]]}),
    ("prec-26", "SELECT CAST(FIRST(ts) AS BIGINT), CAST(LAST(ts) AS BIGINT) FROM {L_ns}.t",
        {"expect": [[_ROWS[0][2], _ROWS[-1][2]]]}),
    # ── ELAPSED — duration across precisions ──
    ("prec-27", "SELECT ELAPSED(ts, 1s) FROM {L_ms}.t"),
    ("prec-28", "SELECT ELAPSED(ts, 1s) FROM {L_us}.t"),
    ("prec-29", "SELECT ELAPSED(ts, 1s) FROM {L_ns}.t"),
    ("prec-30", "SELECT ELAPSED(time, 1s) FROM {I}.sensor"),
    # ── SPREAD — timestamp range per precision ──
    ("prec-31", "SELECT SPREAD(ts) FROM {L_ms}.t"),
    ("prec-32", "SELECT SPREAD(ts) FROM {L_us}.t"),
    ("prec-33", "SELECT SPREAD(ts) FROM {L_ns}.t"),
    ("prec-34", "SELECT SPREAD(time) FROM {I}.sensor"),
    # ── CAST round-trip: ts → BIGINT → TIMESTAMP → BIGINT ──
    ("prec-35", "SELECT CAST(CAST(CAST(ts AS BIGINT) AS TIMESTAMP) AS BIGINT) FROM {L_ms}.t ORDER BY ts",
        {"expect": [[r[0]] for r in _ROWS]}),
    ("prec-36", "SELECT CAST(CAST(CAST(ts AS BIGINT) AS TIMESTAMP) AS BIGINT) FROM {L_us}.t ORDER BY ts",
        {"expect": [[r[1]] for r in _ROWS]}),
    ("prec-37", "SELECT CAST(CAST(CAST(ts AS BIGINT) AS TIMESTAMP) AS BIGINT) FROM {L_ns}.t ORDER BY ts",
        {"expect": [[r[2]] for r in _ROWS]}),
    ("prec-38", "SELECT CAST(CAST(CAST(time AS BIGINT) AS TIMESTAMP) AS BIGINT) FROM {I}.sensor ORDER BY time"),
    # ── _wstart pseudo-column — INTERVAL window precision ──
    ("prec-39", "SELECT CAST(_wstart AS BIGINT), COUNT(*) FROM {L_ms}.t INTERVAL(1m) ORDER BY _wstart",
        {"expect": [[r[0] // 60000 * 60000, 1] for r in _ROWS]}),
    ("prec-40", "SELECT CAST(_wstart AS BIGINT), COUNT(*) FROM {L_us}.t INTERVAL(1m) ORDER BY _wstart",
        {"expect": [[r[1] // 60_000_000 * 60_000_000, 1] for r in _ROWS]}),
    ("prec-41", "SELECT CAST(_wstart AS BIGINT), COUNT(*) FROM {L_ns}.t INTERVAL(1m) ORDER BY _wstart",
        {"expect": [[r[2] // 60_000_000_000 * 60_000_000_000, 1] for r in _ROWS]}),
    ("prec-42", "SELECT CAST(_wstart AS BIGINT), COUNT(*) FROM {I}.sensor INTERVAL(1m) ORDER BY _wstart"),
    # ── Constant CAST — epoch interpretation per precision context ──
    ("prec-43", "SELECT CAST(CAST(1704067200000 AS TIMESTAMP) AS BIGINT) FROM {L_ms}.t LIMIT 1",
        {"expect": [[1704067200000]]}),
    ("prec-44", "SELECT CAST(CAST(1704067200000000 AS TIMESTAMP) AS BIGINT) FROM {L_us}.t LIMIT 1",
        {"expect": [[1704067200000000]]}),
    ("prec-45", "SELECT CAST(CAST(1704067200000000000 AS TIMESTAMP) AS BIGINT) FROM {L_ns}.t LIMIT 1",
        {"expect": [[1704067200000000000]]}),
    # ── TIMEDIFF — duration result precision ──
    ("prec-46", "SELECT TIMEDIFF(MAX(ts), MIN(ts), 1s) FROM {L_ms}.t"),
    ("prec-47", "SELECT TIMEDIFF(MAX(ts), MIN(ts), 1s) FROM {L_us}.t"),
    ("prec-48", "SELECT TIMEDIFF(MAX(ts), MIN(ts), 1s) FROM {L_ns}.t"),
    ("prec-49", "SELECT TIMEDIFF(MAX(time), MIN(time), 1s) FROM {I}.sensor"),
    # ── DIFF(ts) — row interval depends on precision ──
    ("prec-50", "SELECT DIFF(ts) FROM {L_ms}.t",
        {"expect": [[_ROWS[i+1][0] - _ROWS[i][0]] for i in range(len(_ROWS)-1)]}),
    ("prec-51", "SELECT DIFF(ts) FROM {L_us}.t",
        {"expect": [[_ROWS[i+1][1] - _ROWS[i][1]] for i in range(len(_ROWS)-1)]}),
    ("prec-52", "SELECT DIFF(ts) FROM {L_ns}.t",
        {"expect": [[_ROWS[i+1][2] - _ROWS[i][2]] for i in range(len(_ROWS)-1)]}),
    # ── Subquery precision propagation ──
    ("prec-53", "SELECT CAST(ts AS BIGINT) FROM (SELECT ts FROM {L_us}.t WHERE val > 2 ORDER BY ts)",
        {"expect": [[r[1]] for r in _ROWS if r[3] > 2]}),
    ("prec-54", "SELECT CAST(ts AS BIGINT) FROM (SELECT ts FROM {L_ns}.t WHERE val > 2 ORDER BY ts)",
        {"expect": [[r[2]] for r in _ROWS if r[3] > 2]}),
    ("prec-55", "SELECT CAST(time AS BIGINT) FROM (SELECT time FROM {I}.sensor WHERE val > 2 ORDER BY time)"),
    # ── LAG/LEAD — shifted timestamp precision ──
    ("prec-56", "SELECT CAST(LAG(ts, 1) AS BIGINT) FROM {L_us}.t"),
    ("prec-57", "SELECT CAST(LEAD(ts, 1) AS BIGINT) FROM {L_ns}.t"),
    # ── NOW()/TODAY() — basic WHERE functionality (NOT precision-sensitive) ──
    # These verify NOW()/TODAY() works in WHERE, but since test data is from 2024
    # (far in the past), the result is always 5 regardless of NOW's precision.
    # For precision-sensitive filtering tests, see prec-150~161.
    ("prec-60", "SELECT COUNT(*) FROM {L_ms}.t WHERE ts < NOW()", {"expect": _E_COUNT5}),
    ("prec-61", "SELECT COUNT(*) FROM {L_us}.t WHERE ts < NOW()", {"expect": _E_COUNT5}),
    ("prec-62", "SELECT COUNT(*) FROM {L_ns}.t WHERE ts < NOW()", {"expect": _E_COUNT5}),
    ("prec-63", "SELECT COUNT(*) FROM {I}.sensor WHERE time < NOW()", {"expect": _E_COUNT5}),
    ("prec-64", "SELECT COUNT(*) FROM {P}.pk_tstz WHERE ts < NOW()", {"expect": _E_COUNT5}),
    ("prec-65", "SELECT COUNT(*) FROM {L_ms}.t WHERE ts < TODAY()", {"expect": _E_COUNT5}),
    ("prec-66", "SELECT COUNT(*) FROM {L_us}.t WHERE ts < TODAY()", {"expect": _E_COUNT5}),
    ("prec-67", "SELECT COUNT(*) FROM {L_ns}.t WHERE ts < TODAY()", {"expect": _E_COUNT5}),
    ("prec-68", "SELECT COUNT(*) FROM (SELECT * FROM {L_us}.t WHERE ts < NOW())", {"expect": _E_COUNT5}),
    ("prec-69", "SELECT COUNT(*) FROM (SELECT * FROM {L_ns}.t WHERE ts < NOW())", {"expect": _E_COUNT5}),
    # ── SELECT NOW()/TODAY() — comparison against data verifies precision path ──
    ("prec-84", "SELECT NOW() > MAX(ts) FROM {L_ms}.t", {"expect": _E_TRUE}),
    ("prec-85", "SELECT NOW() > MAX(ts) FROM {L_us}.t", {"expect": _E_TRUE}),
    ("prec-86", "SELECT NOW() > MAX(ts) FROM {L_ns}.t", {"expect": _E_TRUE}),
    ("prec-87", "SELECT TODAY() > MAX(ts) FROM {L_ms}.t", {"expect": _E_TRUE}),
    ("prec-88", "SELECT TODAY() > MAX(ts) FROM {L_us}.t", {"expect": _E_TRUE}),
    ("prec-89", "SELECT TODAY() > MAX(ts) FROM {L_ns}.t", {"expect": _E_TRUE}),
    ("prec-90", "SELECT NOW() > MAX(time) FROM {I}.sensor", {"expect": _E_TRUE}),
    ("prec-91", "SELECT NOW() > MAX(ts) FROM {P}.pk_tstz", {"expect": _E_TRUE}),
    ("prec-92", "SELECT TODAY() > MAX(time) FROM {I}.sensor", {"expect": _E_TRUE}),
    # ── NOW()/TODAY() as filter with data projection ──
    ("prec-93", "SELECT val FROM {L_ms}.t WHERE ts < NOW() ORDER BY val", {"expect": _E_VALS}),
    ("prec-94", "SELECT val FROM {L_us}.t WHERE NOW() > ts ORDER BY val", {"expect": _E_VALS}),
    ("prec-95", "SELECT val FROM {L_ns}.t WHERE ts < NOW() ORDER BY val", {"expect": _E_VALS}),
    ("prec-96", "SELECT val FROM {L_ms}.t WHERE ts < TODAY() ORDER BY val", {"expect": _E_VALS}),
    # ── NOW() vs TODAY() relationship ──
    ("prec-97", "SELECT NOW() >= TODAY() FROM {L_ms}.t LIMIT 1", {"expect": _E_TRUE}),
    ("prec-98", "SELECT NOW() >= TODAY() FROM {L_us}.t LIMIT 1", {"expect": _E_TRUE}),
    ("prec-99", "SELECT NOW() >= TODAY() FROM {L_ns}.t LIMIT 1", {"expect": _E_TRUE}),
    # ── NOW()/TODAY() precision verification via stable LENGTH ──
    ("prec-100", "SELECT LENGTH(TO_ISO8601(NOW())) FROM {L_ms}.t LIMIT 1", {"expect": [[28]]}),
    ("prec-101", "SELECT LENGTH(TO_ISO8601(NOW())) FROM {L_us}.t LIMIT 1", {"expect": [[31]]}),
    ("prec-102", "SELECT LENGTH(TO_ISO8601(NOW())) FROM {L_ns}.t LIMIT 1", {"expect": [[34]]}),
    ("prec-103", "SELECT LENGTH(TO_ISO8601(TODAY())) FROM {L_ms}.t LIMIT 1", {"expect": [[28]]}),
    ("prec-104", "SELECT LENGTH(TO_ISO8601(TODAY())) FROM {L_us}.t LIMIT 1", {"expect": [[31]]}),
    ("prec-105", "SELECT LENGTH(TO_ISO8601(TODAY())) FROM {L_ns}.t LIMIT 1", {"expect": [[34]]}),
    # ── CAST(NOW()/TODAY() AS BIGINT) — epoch digit count reveals precision ──
    ("prec-106", "SELECT LENGTH(CAST(CAST(NOW() AS BIGINT) AS VARCHAR(30))) FROM {L_ms}.t LIMIT 1", {"expect": [[13]]}),
    ("prec-107", "SELECT LENGTH(CAST(CAST(NOW() AS BIGINT) AS VARCHAR(30))) FROM {L_us}.t LIMIT 1", {"expect": [[16]]}),
    ("prec-108", "SELECT LENGTH(CAST(CAST(NOW() AS BIGINT) AS VARCHAR(30))) FROM {L_ns}.t LIMIT 1", {"expect": [[19]]}),
    ("prec-109", "SELECT LENGTH(CAST(CAST(TODAY() AS BIGINT) AS VARCHAR(30))) FROM {L_ms}.t LIMIT 1", {"expect": [[13]]}),
    ("prec-110", "SELECT LENGTH(CAST(CAST(TODAY() AS BIGINT) AS VARCHAR(30))) FROM {L_us}.t LIMIT 1", {"expect": [[16]]}),
    ("prec-111", "SELECT LENGTH(CAST(CAST(TODAY() AS BIGINT) AS VARCHAR(30))) FROM {L_ns}.t LIMIT 1", {"expect": [[19]]}),
    # ── Direct SELECT NOW()/TODAY() — projected as columns, precision verified via BIGINT digits ──
    ("prec-130", "SELECT NOW(), LENGTH(CAST(CAST(NOW() AS BIGINT) AS VARCHAR(30))) FROM {L_ms}.t LIMIT 1",
        {"non_deterministic": True, "expect": [[_ANY, 13]]}),
    ("prec-131", "SELECT NOW(), LENGTH(CAST(CAST(NOW() AS BIGINT) AS VARCHAR(30))) FROM {L_us}.t LIMIT 1",
        {"non_deterministic": True, "expect": [[_ANY, 16]]}),
    ("prec-132", "SELECT NOW(), LENGTH(CAST(CAST(NOW() AS BIGINT) AS VARCHAR(30))) FROM {L_ns}.t LIMIT 1",
        {"non_deterministic": True, "expect": [[_ANY, 19]]}),
    ("prec-133", "SELECT TODAY(), LENGTH(CAST(CAST(TODAY() AS BIGINT) AS VARCHAR(30))) FROM {L_ms}.t LIMIT 1",
        {"non_deterministic": True, "expect": [[_ANY, 13]]}),
    ("prec-134", "SELECT TODAY(), LENGTH(CAST(CAST(TODAY() AS BIGINT) AS VARCHAR(30))) FROM {L_us}.t LIMIT 1",
        {"non_deterministic": True, "expect": [[_ANY, 16]]}),
    ("prec-135", "SELECT TODAY(), LENGTH(CAST(CAST(TODAY() AS BIGINT) AS VARCHAR(30))) FROM {L_ns}.t LIMIT 1",
        {"non_deterministic": True, "expect": [[_ANY, 19]]}),
    # ── SELECT NOW()/TODAY() from external sources ──
    # Per FS §3.3: single-source query precision follows the source's native
    # precision.  MySQL/PG = µs (16 digits), InfluxDB = ns (19 digits).
    ("prec-136", "SELECT NOW(), LENGTH(CAST(CAST(NOW() AS BIGINT) AS VARCHAR(30))) FROM {I}.sensor LIMIT 1",
        {"non_deterministic": True, "expect": [[_ANY, 19]]}),
    ("prec-137", "SELECT NOW(), LENGTH(CAST(CAST(NOW() AS BIGINT) AS VARCHAR(30))) FROM {P}.pk_tstz LIMIT 1",
        {"non_deterministic": True, "expect": [[_ANY, 16]]}),
    ("prec-138", "SELECT TODAY(), LENGTH(CAST(CAST(TODAY() AS BIGINT) AS VARCHAR(30))) FROM {I}.sensor LIMIT 1",
        {"non_deterministic": True, "expect": [[_ANY, 19]]}),
    ("prec-139", "SELECT TODAY(), LENGTH(CAST(CAST(TODAY() AS BIGINT) AS VARCHAR(30))) FROM {P}.pk_tstz LIMIT 1",
        {"non_deterministic": True, "expect": [[_ANY, 16]]}),
    # ── NOW()/TODAY() in subquery — precision propagation ──
    ("prec-140", "SELECT NOW(), LENGTH(CAST(CAST(NOW() AS BIGINT) AS VARCHAR(30))) FROM (SELECT * FROM {L_us}.t LIMIT 1)",
        {"non_deterministic": True, "expect": [[_ANY, 16]]}),
    ("prec-141", "SELECT TODAY(), LENGTH(CAST(CAST(TODAY() AS BIGINT) AS VARCHAR(30))) FROM (SELECT * FROM {L_ns}.t LIMIT 1)",
        {"non_deterministic": True, "expect": [[_ANY, 19]]}),
    # ── NOW() in expressions — arithmetic with timestamps ──
    ("prec-112", "SELECT COUNT(*) FROM {L_ms}.t WHERE ts BETWEEN '2024-01-01' AND NOW()", {"expect": _E_COUNT5}),
    ("prec-113", "SELECT COUNT(*) FROM {L_us}.t WHERE ts BETWEEN '2024-01-01' AND NOW()", {"expect": _E_COUNT5}),
    ("prec-114", "SELECT COUNT(*) FROM {L_ns}.t WHERE ts BETWEEN '2024-01-01' AND NOW()", {"expect": _E_COUNT5}),
    ("prec-115", "SELECT TIMEDIFF(NOW(), MAX(ts), 1s) > 0 FROM {L_ms}.t", {"expect": _E_TRUE}),
    ("prec-116", "SELECT TIMEDIFF(NOW(), MAX(ts), 1s) > 0 FROM {L_us}.t", {"expect": _E_TRUE}),
    # ── NOW()/TODAY() precision-sensitive epoch range filtering ──
    # Unlike prec-60~69 (which pass regardless of NOW precision since test data
    # is 2+ years old), these cases use CAST(NOW() AS BIGINT) epoch range in
    # WHERE so that the query result is CORRECT only when precision is CORRECT.
    # If NOW() returns wrong precision, the epoch value falls outside the expected
    # range and the query returns 0 instead of 5, catching the precision bug.
    #
    # Epoch ranges by precision:
    #   ms = [1e12, 1e13)   ~year 2001-2286
    #   µs = [1e15, 1e16)   ~year 2001-2286
    #   ns = [1e18, 4e18)   ~year 2001-2096   (upper < INT64_MAX)
    #
    # Positive: NOW/TODAY epoch in CORRECT precision range → 5 rows
    ("prec-150", "SELECT COUNT(*) FROM {L_ms}.t WHERE CAST(NOW() AS BIGINT) BETWEEN 1000000000000 AND 9999999999999",
        {"expect": _E_COUNT5}),
    ("prec-151", "SELECT COUNT(*) FROM {L_us}.t WHERE CAST(NOW() AS BIGINT) BETWEEN 1000000000000000 AND 9999999999999999",
        {"expect": _E_COUNT5}),
    ("prec-152", "SELECT COUNT(*) FROM {L_ns}.t WHERE CAST(NOW() AS BIGINT) BETWEEN 1000000000000000000 AND 4000000000000000000",
        {"expect": _E_COUNT5}),
    ("prec-153", "SELECT COUNT(*) FROM {L_ms}.t WHERE CAST(TODAY() AS BIGINT) BETWEEN 1000000000000 AND 9999999999999",
        {"expect": _E_COUNT5}),
    ("prec-154", "SELECT COUNT(*) FROM {L_us}.t WHERE CAST(TODAY() AS BIGINT) BETWEEN 1000000000000000 AND 9999999999999999",
        {"expect": _E_COUNT5}),
    ("prec-155", "SELECT COUNT(*) FROM {L_ns}.t WHERE CAST(TODAY() AS BIGINT) BETWEEN 1000000000000000000 AND 4000000000000000000",
        {"expect": _E_COUNT5}),
    # External sources: InfluxDB(ns), PG(µs)
    ("prec-156", "SELECT COUNT(*) FROM {I}.sensor WHERE CAST(NOW() AS BIGINT) BETWEEN 1000000000000000000 AND 4000000000000000000",
        {"expect": _E_COUNT5}),
    ("prec-157", "SELECT COUNT(*) FROM {P}.pk_tstz WHERE CAST(NOW() AS BIGINT) BETWEEN 1000000000000000 AND 9999999999999999",
        {"expect": _E_COUNT5}),
    # Negative guard: NOW epoch in WRONG precision range → empty result.
    # Proves that NOW() truly uses the expected precision, not a different one.
    # e.g. if ms-table NOW() were erroneously µs, it would be 16 digits and
    # fall into the µs range, but prec-158 checks the µs range on ms table → empty.
    # (TDengine returns empty result set, not [[0]], when constant WHERE is false.)
    ("prec-158", "SELECT COUNT(*) FROM {L_ms}.t WHERE CAST(NOW() AS BIGINT) BETWEEN 1000000000000000 AND 9999999999999999",
        {"expect": []}),
    ("prec-159", "SELECT COUNT(*) FROM {L_us}.t WHERE CAST(NOW() AS BIGINT) BETWEEN 1000000000000 AND 9999999999999",
        {"expect": []}),
    ("prec-160", "SELECT COUNT(*) FROM {L_ns}.t WHERE CAST(NOW() AS BIGINT) BETWEEN 1000000000000 AND 9999999999999",
        {"expect": []}),
    ("prec-161", "SELECT COUNT(*) FROM {L_ns}.t WHERE CAST(NOW() AS BIGINT) BETWEEN 1000000000000000 AND 9999999999999999",
        {"expect": []}),
    # ── TO_ISO8601 with fixed offset — fractional seconds reveal precision ──
    ("prec-70", "SELECT TO_ISO8601(ts, '+00:00') FROM {L_ms}.t ORDER BY ts"),
    ("prec-71", "SELECT TO_ISO8601(ts, '+00:00') FROM {L_us}.t ORDER BY ts"),
    ("prec-72", "SELECT TO_ISO8601(ts, '+00:00') FROM {L_ns}.t ORDER BY ts"),
    ("prec-73", "SELECT TO_ISO8601(time, '+00:00') FROM {I}.sensor ORDER BY time"),
    ("prec-74", "SELECT TO_ISO8601(ts, '+00:00') FROM {P}.pk_tstz ORDER BY ts"),
    # ── WHERE exact epoch — precision-specific filter path ──
    ("prec-75", "SELECT val FROM {L_ms}.t WHERE ts = 1704067320123",
        {"expect": [[3]]}),
    ("prec-76", "SELECT val FROM {L_us}.t WHERE ts = 1704067320123456",
        {"expect": [[3]]}),
    ("prec-77", "SELECT val FROM {L_ns}.t WHERE ts = 1704067320123456789",
        {"expect": [[3]]}),
    # Cross-precision epoch on ms table (µs epoch — NOT auto-truncated)
    ("prec-78", "SELECT val FROM {L_ms}.t WHERE ts = 1704067320123000"),
    # ns epoch on µs table (NOT auto-truncated)
    ("prec-79", "SELECT val FROM {L_us}.t WHERE ts = 1704067320123456000"),
    # ── TIMEDIFF with epoch — numeric result shows sub-second precision ──
    ("prec-80", "SELECT TIMEDIFF(ts, 1704067200000, 1a) FROM {L_ms}.t ORDER BY ts",
        {"expect": [[r[0] - _ROWS[0][0]] for r in _ROWS]}),
    ("prec-81", "SELECT TIMEDIFF(ts, 1704067200000000, 1u) FROM {L_us}.t ORDER BY ts",
        {"expect": [[r[1] - _ROWS[0][1]] for r in _ROWS]}),
    ("prec-82", "SELECT TIMEDIFF(ts, 1704067200000000000, 1b) FROM {L_ns}.t ORDER BY ts",
        {"expect": [[r[2] - _ROWS[0][2]] for r in _ROWS]}),
    ("prec-83", "SELECT TIMEDIFF(time, 1704067200000000000, 1b) FROM {I}.sensor ORDER BY time"),
]

# ── PREC-TZ group (TZ-sensitive precision cases; tested in test_timezone) ──
# prec-01/02/03: CAST of tz-naive DATETIME/TIMESTAMP WITHOUT TZ uses clientTz.
# prec-09/10: multi_ts has tz-naive columns whose epoch depends on clientTz.
# prec-11–13: TO_ISO8601 includes tz offset (+0800 vs +0000).
_PREC_TZ_CASES = [
    ("prec-01", "SELECT CAST(ts AS BIGINT), val FROM {M}.pk_dt ORDER BY ts"),
    ("prec-02", "SELECT CAST(ts AS BIGINT), val FROM {M}.pk_ts ORDER BY ts"),
    ("prec-03", "SELECT CAST(ts AS BIGINT), val FROM {P}.pk_ts ORDER BY ts"),
    ("prec-09", "SELECT CAST(ts AS BIGINT), CAST(ts_aware AS BIGINT), CAST(ts_naive AS BIGINT) FROM {M}.multi_ts ORDER BY ts"),
    ("prec-10", "SELECT CAST(ts AS BIGINT), CAST(ts_aware AS BIGINT), CAST(ts_naive AS BIGINT) FROM {P}.multi_ts ORDER BY ts"),
    ("prec-11", "SELECT TO_ISO8601(ts) FROM {M}.pk_dt ORDER BY ts"),
    ("prec-12", "SELECT TO_ISO8601(ts) FROM {P}.pk_ts ORDER BY ts"),
    ("prec-13", "SELECT TO_ISO8601(time) FROM {I}.sensor ORDER BY time"),
    # ── TIMETRUNCATE on TZ-dependent sources ──
    ("prec-t14", "SELECT CAST(TIMETRUNCATE(ts, 1s) AS BIGINT) FROM {M}.pk_dt ORDER BY ts"),
    ("prec-t15", "SELECT CAST(TIMETRUNCATE(ts, 1s) AS BIGINT) FROM {M}.pk_ts ORDER BY ts"),
    ("prec-t16", "SELECT CAST(TIMETRUNCATE(ts, 1s) AS BIGINT) FROM {P}.pk_ts ORDER BY ts"),
    # ── MIN/MAX on TZ-dependent sources ──
    ("prec-t17", "SELECT CAST(MIN(ts) AS BIGINT), CAST(MAX(ts) AS BIGINT) FROM {M}.pk_dt"),
    ("prec-t18", "SELECT CAST(MIN(ts) AS BIGINT), CAST(MAX(ts) AS BIGINT) FROM {M}.pk_ts"),
    ("prec-t19", "SELECT CAST(MIN(ts) AS BIGINT), CAST(MAX(ts) AS BIGINT) FROM {P}.pk_ts"),
    # ── Subquery precision on TZ-dependent sources ──
    ("prec-t20", "SELECT CAST(ts AS BIGINT) FROM (SELECT ts FROM {M}.pk_dt WHERE val > 2 ORDER BY ts)"),
    ("prec-t21", "SELECT CAST(ts AS BIGINT) FROM (SELECT ts FROM {P}.pk_ts WHERE val > 2 ORDER BY ts)"),
    # ── NOW()/TODAY() on TZ-sensitive sources ──
    ("prec-t22", "SELECT COUNT(*) FROM {M}.pk_dt WHERE ts < NOW()", {"expect": _E_COUNT5}),
    ("prec-t23", "SELECT COUNT(*) FROM {P}.pk_ts WHERE ts < NOW()", {"expect": _E_COUNT5}),
    ("prec-t24", "SELECT COUNT(*) FROM {M}.pk_dt WHERE ts < TODAY()", {"expect": _E_COUNT5}),
    # ── TO_ISO8601('+00:00') on TZ-sensitive — epoch varies by clientTz ──
    ("prec-t25", "SELECT TO_ISO8601(ts, '+00:00') FROM {M}.pk_dt ORDER BY ts"),
    ("prec-t26", "SELECT TO_ISO8601(ts, '+00:00') FROM {M}.pk_ts ORDER BY ts"),
    ("prec-t27", "SELECT TO_ISO8601(ts, '+00:00') FROM {P}.pk_ts ORDER BY ts"),
    # ── WHERE exact epoch on TZ-sensitive sources ──
    ("prec-t28", "SELECT val FROM {M}.pk_dt WHERE ts = 1704067320123"),
    ("prec-t29", "SELECT val FROM {P}.pk_ts WHERE ts = 1704067320123456"),
    # ── TIMEDIFF on TZ-sensitive sources — numeric precision diff ──
    ("prec-t30", "SELECT TIMEDIFF(ts, 1704067200000, 1a) FROM {M}.pk_dt ORDER BY ts"),
    ("prec-t31", "SELECT TIMEDIFF(ts, 1704067200000000, 1u) FROM {P}.pk_ts ORDER BY ts"),
    # ── SELECT NOW()/TODAY() on TZ-sensitive sources — comparison verifies path ──
    ("prec-t32", "SELECT NOW() > MAX(ts) FROM {M}.pk_dt", {"expect": _E_TRUE}),
    ("prec-t33", "SELECT NOW() > MAX(ts) FROM {M}.pk_ts", {"expect": _E_TRUE}),
    ("prec-t34", "SELECT NOW() > MAX(ts) FROM {P}.pk_ts", {"expect": _E_TRUE}),
    ("prec-t35", "SELECT TODAY() > MAX(ts) FROM {M}.pk_dt", {"expect": _E_TRUE}),
    ("prec-t36", "SELECT TODAY() > MAX(ts) FROM {P}.pk_ts", {"expect": _E_TRUE}),
    # ── NOW()/TODAY() as filter on TZ-sensitive sources ──
    ("prec-t37", "SELECT val FROM {M}.pk_dt WHERE ts < NOW() ORDER BY val", {"expect": _E_VALS}),
    ("prec-t38", "SELECT val FROM {P}.pk_ts WHERE ts < TODAY() ORDER BY val", {"expect": _E_VALS}),
    # ── Direct SELECT NOW()/TODAY() on TZ-sensitive sources ──
    # Per FS §3.3: MySQL native precision = µs (16 digits).
    ("prec-t39", "SELECT NOW(), LENGTH(CAST(CAST(NOW() AS BIGINT) AS VARCHAR(30))) FROM {M}.pk_dt LIMIT 1",
        {"non_deterministic": True, "expect": [[_ANY, 16]]}),
    ("prec-t40", "SELECT NOW(), LENGTH(CAST(CAST(NOW() AS BIGINT) AS VARCHAR(30))) FROM {M}.pk_ts LIMIT 1",
        {"non_deterministic": True, "expect": [[_ANY, 16]]}),
    ("prec-t41", "SELECT NOW(), LENGTH(CAST(CAST(NOW() AS BIGINT) AS VARCHAR(30))) FROM {P}.pk_ts LIMIT 1",
        {"non_deterministic": True, "expect": [[_ANY, 16]]}),
    ("prec-t42", "SELECT TODAY(), LENGTH(CAST(CAST(TODAY() AS BIGINT) AS VARCHAR(30))) FROM {M}.pk_dt LIMIT 1",
        {"non_deterministic": True, "expect": [[_ANY, 16]]}),
    ("prec-t43", "SELECT TODAY(), LENGTH(CAST(CAST(TODAY() AS BIGINT) AS VARCHAR(30))) FROM {P}.pk_ts LIMIT 1",
        {"non_deterministic": True, "expect": [[_ANY, 16]]}),
    # ── Precision-sensitive epoch range filtering on TZ-sensitive sources ──
    # MySQL/PG native precision = µs.  NOW() epoch must fall in µs range [1e15,1e16).
    # Positive: µs range → 5 rows
    ("prec-t44", "SELECT COUNT(*) FROM {M}.pk_dt WHERE CAST(NOW() AS BIGINT) BETWEEN 1000000000000000 AND 9999999999999999",
        {"expect": _E_COUNT5}),
    ("prec-t45", "SELECT COUNT(*) FROM {P}.pk_ts WHERE CAST(NOW() AS BIGINT) BETWEEN 1000000000000000 AND 9999999999999999",
        {"expect": _E_COUNT5}),
    # Negative: ms range → empty result (proves NOT ms precision)
    ("prec-t46", "SELECT COUNT(*) FROM {M}.pk_dt WHERE CAST(NOW() AS BIGINT) BETWEEN 1000000000000 AND 9999999999999",
        {"expect": []}),
    ("prec-t47", "SELECT COUNT(*) FROM {P}.pk_ts WHERE CAST(NOW() AS BIGINT) BETWEEN 1000000000000 AND 9999999999999",
        {"expect": []}),
]

# ── XPREC group (cross-precision operations — TZ-insensitive only) ──
# TZ-sensitive cases moved to _XPREC_TZ_CASES (all involve MySQL pk_dt DATETIME
# or PG pk_ts TIMESTAMP WITHOUT TZ whose epoch interpretation varies by clientTz,
# or output raw TIMESTAMP values whose rendering depends on client timezone).
_XPREC_CASES = [
    # Comparison operators
    ("xprec-02", "SELECT a.val FROM {L_ms}.t a, {I}.sensor b WHERE a.ts = b.time ORDER BY a.val"),
    ("xprec-05", "SELECT val FROM {I}.sensor WHERE time > (SELECT MIN(ts) FROM {L_ms}.t) ORDER BY time"),
    ("xprec-07", "SELECT val FROM {L_ms}.t WHERE ts >= (SELECT MIN(ts) FROM {M}.pk_dt) ORDER BY ts"),
    ("xprec-08", "SELECT val FROM {L_ms}.t WHERE ts <= (SELECT MAX(time) FROM {I}.sensor) ORDER BY ts"),
    ("xprec-09", "SELECT val FROM {L_ms}.t WHERE ts <> (SELECT MIN(ts) FROM {M}.pk_dt) ORDER BY ts"),
    ("xprec-11", "SELECT val FROM {I}.sensor WHERE time BETWEEN (SELECT MIN(ts) FROM {L_ms}.t) AND (SELECT MAX(ts) FROM {L_ms}.t) ORDER BY time"),
    # IN / NOT IN (subquery)
    ("xprec-20", "SELECT val FROM {M}.pk_dt WHERE ts IN (SELECT ts FROM {L_ms}.t) ORDER BY ts"),
    ("xprec-21", "SELECT val FROM {I}.sensor WHERE time IN (SELECT ts FROM {L_ms}.t) ORDER BY time"),
    ("xprec-22", "SELECT val FROM {I}.sensor WHERE time IN (SELECT ts FROM {M}.pk_dt) ORDER BY time"),
    ("xprec-23", "SELECT val FROM {M}.pk_dt WHERE ts NOT IN (SELECT ts FROM {L_ms}.t WHERE val > 3) ORDER BY ts"),
    ("xprec-24", "SELECT val FROM {I}.sensor WHERE time NOT IN (SELECT ts FROM {L_ms}.t WHERE val > 3) ORDER BY time"),
    ("xprec-25", "SELECT val FROM {I}.sensor WHERE time NOT IN (SELECT ts FROM {M}.pk_dt WHERE val > 3) ORDER BY time"),
    # IN / NOT IN (constant epoch lists)
    ("xprec-26", "SELECT val FROM {M}.pk_dt WHERE ts IN (1704067200000, 1704067260000) ORDER BY ts"),
    ("xprec-27", "SELECT val FROM {L_ms}.t WHERE ts IN (1704067200000000, 1704067260000000) ORDER BY ts"),
    ("xprec-28", "SELECT val FROM {L_ms}.t WHERE ts IN (1704067200000000000, 1704067260000000000) ORDER BY ts"),
    ("xprec-29", "SELECT val FROM {M}.pk_dt WHERE ts NOT IN (1704067200000, 1704067320123) ORDER BY ts"),
    # UNION ALL
    ("xprec-31", "SELECT ts, val FROM {L_ms}.t UNION ALL SELECT time, val FROM {I}.sensor ORDER BY 1"),
    # xprec-34 moved to _XPREC_TZ_CASES (outputs raw TIMESTAMP — TZ-dependent)
    ("xprec-35", "SELECT ts, val FROM {L_ms}.t UNION SELECT ts, val FROM {L_ns}.t ORDER BY 1"),
    ("xprec-36", "SELECT ts, val FROM {L_us}.t UNION SELECT ts, val FROM {L_ns}.t ORDER BY 1"),
    # JOIN
    ("xprec-41", "SELECT a.val, b.val FROM {L_ms}.t a INNER JOIN {I}.sensor b ON a.ts = b.time ORDER BY a.val"),
    ("xprec-44", "SELECT a.val, b.val FROM {L_ms}.t a LEFT JOIN {I}.sensor b ON a.ts = b.time ORDER BY a.val"),
    ("xprec-47", "SELECT COALESCE(a.val, b.val) FROM {L_ms}.t a FULL JOIN {I}.sensor b ON a.ts = b.time ORDER BY 1"),
    # CASE WHEN
    # xprec-61 moved to _XPREC_TZ_CASES (outputs raw TIMESTAMP — TZ-dependent)
    # FROM subquery — verify subquery output precision
    ("xprec-72", "SELECT CAST(time AS BIGINT) FROM (SELECT time FROM {I}.sensor ORDER BY time)"),
    ("xprec-73", "SELECT CAST(ts AS BIGINT) FROM (SELECT ts FROM {L_us}.t ORDER BY ts)"),
    # FROM subquery — nested subquery precision
    ("xprec-81", "SELECT CAST(ts AS BIGINT) FROM (SELECT ts FROM (SELECT ts, val FROM {M}.pk_dt) WHERE val > 2)"),
    ("xprec-82", "SELECT CAST(ts AS BIGINT) FROM (SELECT ts FROM (SELECT ts, val FROM {P}.pk_ts) WHERE val > 2)"),
    ("xprec-83", "SELECT TO_ISO8601(ts) FROM (SELECT ts FROM (SELECT ts, val FROM {M}.pk_dt) WHERE val <= 3 ORDER BY ts)"),
    # ── Local cross-precision JOINs ──
    # ms/µs: rows 3,4 have sub-ms fractions → only aligned rows match
    ("xprec-90", "SELECT a.val, b.val FROM {L_ms}.t a INNER JOIN {L_us}.t b ON a.ts = b.ts ORDER BY a.val"),
    # ms/ns: rows 3,4 have sub-ms fractions → only aligned rows match
    ("xprec-91", "SELECT a.val, b.val FROM {L_ms}.t a INNER JOIN {L_ns}.t b ON a.ts = b.ts ORDER BY a.val"),
    # µs/ns: rows 3,4 have sub-µs fractions → only aligned rows match
    ("xprec-92", "SELECT a.val, b.val FROM {L_us}.t a INNER JOIN {L_ns}.t b ON a.ts = b.ts ORDER BY a.val"),
    # µs vs InfluxDB ns
    ("xprec-93", "SELECT a.val, b.val FROM {L_us}.t a INNER JOIN {I}.sensor b ON a.ts = b.time ORDER BY a.val"),
    # ns vs InfluxDB ns — all rows should match
    ("xprec-94", "SELECT a.val, b.val FROM {L_ns}.t a INNER JOIN {I}.sensor b ON a.ts = b.time ORDER BY a.val"),
    # ── LEFT JOIN — show NULL for precision-mismatched rows ──
    ("xprec-95", "SELECT a.val, b.val FROM {L_ms}.t a LEFT JOIN {L_us}.t b ON a.ts = b.ts ORDER BY a.val"),
    ("xprec-96", "SELECT a.val, b.val FROM {L_us}.t a LEFT JOIN {L_ns}.t b ON a.ts = b.ts ORDER BY a.val"),
    # ── Cross-precision comparison operators ──
    ("xprec-97", "SELECT val FROM {L_us}.t WHERE ts > (SELECT MIN(ts) FROM {L_ms}.t) ORDER BY ts"),
    ("xprec-98", "SELECT val FROM {L_ns}.t WHERE ts <= (SELECT MAX(ts) FROM {L_us}.t) ORDER BY ts"),
    ("xprec-99", "SELECT val FROM {L_us}.t WHERE ts BETWEEN (SELECT MIN(ts) FROM {L_ms}.t) AND (SELECT MAX(ts) FROM {L_ns}.t) ORDER BY ts"),
    # ── Cross-precision IN/NOT IN ──
    ("xprec-100", "SELECT val FROM {L_us}.t WHERE ts IN (SELECT ts FROM {L_ms}.t) ORDER BY ts"),
    ("xprec-101", "SELECT val FROM {L_ns}.t WHERE ts IN (SELECT ts FROM {L_us}.t) ORDER BY ts"),
    ("xprec-102", "SELECT val FROM {L_ns}.t WHERE ts NOT IN (SELECT ts FROM {L_ms}.t WHERE val > 3) ORDER BY ts"),
    # ── Cross-precision UNION with epoch display ──
    ("xprec-103", "SELECT CAST(ts AS BIGINT), val FROM {L_ms}.t UNION ALL SELECT CAST(ts AS BIGINT), val FROM {L_us}.t ORDER BY 1"),
    ("xprec-104", "SELECT CAST(ts AS BIGINT), val FROM {L_us}.t UNION ALL SELECT CAST(ts AS BIGINT), val FROM {L_ns}.t ORDER BY 1"),
    ("xprec-105", "SELECT CAST(ts AS BIGINT), val FROM {L_ms}.t UNION ALL SELECT CAST(time AS BIGINT), val FROM {I}.sensor ORDER BY 1"),
    # ── Cross-precision subquery nesting ──
    ("xprec-110", "SELECT CAST(ts AS BIGINT) FROM (SELECT ts FROM {L_ms}.t UNION ALL SELECT ts FROM {L_us}.t) ORDER BY 1"),
    ("xprec-111", "SELECT CAST(ts AS BIGINT) FROM (SELECT ts FROM {L_us}.t UNION ALL SELECT ts FROM {L_ns}.t) ORDER BY 1"),
    # ── Cross-precision aggregate comparison ──
    ("xprec-112", "SELECT (SELECT CAST(MIN(ts) AS BIGINT) FROM {L_ms}.t), (SELECT CAST(MIN(ts) AS BIGINT) FROM {L_us}.t), (SELECT CAST(MIN(time) AS BIGINT) FROM {I}.sensor)",
     {"expect_error": "Current sql does not support subquery as expr"}),
    ("xprec-113", "SELECT (SELECT CAST(MAX(ts) AS BIGINT) FROM {L_us}.t), (SELECT CAST(MAX(ts) AS BIGINT) FROM {L_ns}.t)",
     {"expect_error": "Current sql does not support subquery as expr"}),
    # ── Cross-precision TO_ISO8601 from JOIN — visible precision alignment ──
    ("xprec-120", "SELECT TO_ISO8601(a.ts, '+00:00'), TO_ISO8601(b.ts, '+00:00') FROM {L_ms}.t a INNER JOIN {L_us}.t b ON a.ts = b.ts ORDER BY a.ts"),
    ("xprec-121", "SELECT TO_ISO8601(a.ts, '+00:00'), TO_ISO8601(b.ts, '+00:00') FROM {L_us}.t a INNER JOIN {L_ns}.t b ON a.ts = b.ts ORDER BY a.ts"),
    ("xprec-122", "SELECT TO_ISO8601(a.ts, '+00:00'), TO_ISO8601(b.time, '+00:00') FROM {L_ms}.t a INNER JOIN {I}.sensor b ON a.ts = b.time ORDER BY a.ts"),
    # ── Cross-precision TIMEDIFF — verifies alignment in computation path ──
    ("xprec-123", "SELECT TIMEDIFF(a.ts, b.ts, 1s) FROM {L_ms}.t a INNER JOIN {L_us}.t b ON a.ts = b.ts ORDER BY a.ts"),
    ("xprec-124", "SELECT TIMEDIFF(a.ts, b.ts, 1s) FROM {L_us}.t a INNER JOIN {L_ns}.t b ON a.ts = b.ts ORDER BY a.ts"),
    ("xprec-125", "SELECT TIMEDIFF(a.ts, b.time, 1s) FROM {L_ms}.t a INNER JOIN {I}.sensor b ON a.ts = b.time ORDER BY a.ts"),
    # ── NOW()/TODAY() in cross-precision context ──
    ("xprec-126", "SELECT a.val FROM {L_ms}.t a, {L_us}.t b WHERE a.ts = b.ts AND a.ts < NOW() ORDER BY a.val"),
    ("xprec-127", "SELECT COUNT(*) FROM {L_ms}.t a INNER JOIN {L_us}.t b ON a.ts = b.ts WHERE a.ts < NOW()"),
    ("xprec-128", "SELECT COUNT(*) FROM {L_us}.t WHERE ts < NOW() AND ts > (SELECT MIN(ts) FROM {L_ns}.t)"),
    ("xprec-129", "SELECT COUNT(*) FROM {L_ms}.t WHERE ts < TODAY() AND ts IN (SELECT ts FROM {L_us}.t)",
        {"expect": [[3]]}),
    # ── NOW()/TODAY() in cross-precision JOIN — comparison result is deterministic ──
    ("xprec-133", "SELECT NOW() > a.ts, a.val FROM {L_ms}.t a INNER JOIN {L_us}.t b ON a.ts = b.ts ORDER BY a.val"),
    ("xprec-134", "SELECT NOW() > a.ts, a.val FROM {L_us}.t a INNER JOIN {L_ns}.t b ON a.ts = b.ts ORDER BY a.val"),
    ("xprec-135", "SELECT TODAY() > a.ts FROM {L_ms}.t a INNER JOIN {I}.sensor b ON a.ts = b.time ORDER BY a.val"),
    # ── Cross-precision UNION via TO_ISO8601 ──
    ("xprec-130", "SELECT TO_ISO8601(ts, '+00:00'), val FROM {L_ms}.t UNION ALL SELECT TO_ISO8601(ts, '+00:00'), val FROM {L_us}.t ORDER BY 1"),
    ("xprec-131", "SELECT TO_ISO8601(ts, '+00:00'), val FROM {L_us}.t UNION ALL SELECT TO_ISO8601(ts, '+00:00'), val FROM {L_ns}.t ORDER BY 1"),
    ("xprec-132", "SELECT TO_ISO8601(ts, '+00:00'), val FROM {L_ms}.t UNION ALL SELECT TO_ISO8601(time, '+00:00'), val FROM {I}.sensor ORDER BY 1"),
]

# ── XPREC-TZ group (TZ-sensitive cross-precision cases; tested in test_timezone) ──
# Includes: MySQL pk_dt (DATETIME) / PG pk_ts (TIMESTAMP WITHOUT TZ) whose epoch
# interpretation varies by clientTz, AND local-only queries that output raw
# TIMESTAMP values whose rendering depends on client timezone.
_XPREC_TZ_CASES = [
    # Comparison operators — involve MySQL pk_dt
    ("xprec-01", "SELECT a.val FROM {L_ms}.t a, {M}.pk_dt b WHERE a.ts = b.ts ORDER BY a.val"),
    ("xprec-03", "SELECT a.val FROM {M}.pk_dt a, {I}.sensor b WHERE a.ts = b.time ORDER BY a.val"),
    ("xprec-04", "SELECT val FROM {M}.pk_dt WHERE ts > (SELECT MIN(ts) FROM {L_ms}.t) ORDER BY ts"),
    ("xprec-06", "SELECT val FROM {M}.pk_dt WHERE ts < (SELECT MAX(time) FROM {I}.sensor) ORDER BY ts"),
    ("xprec-10", "SELECT val FROM {M}.pk_dt WHERE ts BETWEEN (SELECT MIN(ts) FROM {L_ms}.t) AND (SELECT MAX(ts) FROM {L_ms}.t) ORDER BY ts"),
    ("xprec-12", "SELECT val FROM {I}.sensor WHERE time BETWEEN (SELECT MIN(ts) FROM {M}.pk_dt) AND (SELECT MAX(ts) FROM {M}.pk_dt) ORDER BY time"),
    ("xprec-e30", "SELECT val FROM {M}.pk_dt WHERE ts IN ('2024-01-01 00:00:00', '2024-01-01 00:01:00') ORDER BY ts"),
    # UNION ALL — involve MySQL pk_dt
    ("xprec-30", "SELECT ts, val FROM {L_ms}.t UNION ALL SELECT ts, val FROM {M}.pk_dt ORDER BY 1"),
    ("xprec-32", "SELECT ts, val FROM {M}.pk_dt UNION ALL SELECT time, val FROM {I}.sensor ORDER BY 1"),
    ("xprec-33", "SELECT ts, val FROM {L_ms}.t UNION ALL SELECT ts, val FROM {M}.pk_dt UNION ALL SELECT time, val FROM {I}.sensor ORDER BY 1"),
    # JOIN — involve MySQL pk_dt
    ("xprec-40", "SELECT a.val, b.val FROM {L_ms}.t a INNER JOIN {M}.pk_dt b ON a.ts = b.ts ORDER BY a.val"),
    ("xprec-42", "SELECT a.val, b.val FROM {M}.pk_dt a INNER JOIN {I}.sensor b ON a.ts = b.time ORDER BY a.val"),
    ("xprec-43", "SELECT a.val, b.val FROM {L_ms}.t a LEFT JOIN {M}.pk_dt b ON a.ts = b.ts ORDER BY a.val"),
    ("xprec-45", "SELECT a.val, b.val FROM {M}.pk_dt a LEFT JOIN {I}.sensor b ON a.ts = b.time ORDER BY a.val"),
    ("xprec-46", "SELECT COALESCE(a.val, b.val) FROM {L_ms}.t a FULL JOIN {M}.pk_dt b ON a.ts = b.ts ORDER BY 1"),
    # CASE WHEN — involve MySQL pk_dt
    ("xprec-60", "SELECT CASE WHEN a.val > 3 THEN (SELECT MAX(ts) FROM {M}.pk_dt) ELSE (SELECT MIN(ts) FROM {L_ms}.t) END FROM {L_ms}.t a ORDER BY a.ts"),
    ("xprec-62", "SELECT CASE WHEN a.val > 3 THEN (SELECT MAX(time) FROM {I}.sensor) ELSE (SELECT MIN(ts) FROM {M}.pk_dt) END FROM {M}.pk_dt a ORDER BY a.ts"),
    # FROM subquery — involve MySQL pk_dt or PG pk_ts (TIMESTAMP WITHOUT TZ)
    ("xprec-70", "SELECT CAST(ts AS BIGINT) FROM (SELECT ts FROM {M}.pk_dt ORDER BY ts)"),
    ("xprec-71", "SELECT CAST(ts AS BIGINT) FROM (SELECT ts FROM {P}.pk_ts ORDER BY ts)"),
    ("xprec-74", "SELECT TO_ISO8601(ts) FROM (SELECT ts FROM {M}.pk_dt ORDER BY ts)"),
    ("xprec-75", "SELECT TO_ISO8601(ts) FROM (SELECT ts FROM {P}.pk_ts ORDER BY ts)"),
    ("xprec-76", "SELECT TIMETRUNCATE(ts, 1s) FROM (SELECT ts FROM {M}.pk_dt ORDER BY ts)"),
    ("xprec-77", "SELECT TIMETRUNCATE(ts, 1s) FROM (SELECT ts FROM {P}.pk_ts ORDER BY ts)"),
    ("xprec-78", "SELECT CAST(ts AS BIGINT) FROM (SELECT ts FROM {M}.pk_dt UNION ALL SELECT ts FROM {P}.pk_ts) ORDER BY 1"),
    ("xprec-79", "SELECT CAST(ts AS BIGINT) FROM (SELECT ts FROM {L_ms}.t UNION ALL SELECT ts FROM {M}.pk_dt) ORDER BY 1"),
    ("xprec-80", "SELECT CAST(time AS BIGINT) FROM (SELECT time FROM {I}.sensor UNION ALL SELECT ts AS time FROM {P}.pk_ts) ORDER BY 1"),
    # ── TZ-sensitive cross-precision with non-CAST patterns ──
    ("xprec-t90", "SELECT TO_ISO8601(a.ts, '+00:00'), TO_ISO8601(b.ts, '+00:00') FROM {L_ms}.t a INNER JOIN {M}.pk_dt b ON a.ts = b.ts ORDER BY a.ts"),
    ("xprec-t91", "SELECT TIMEDIFF(a.ts, b.ts, 1s) FROM {L_ms}.t a INNER JOIN {M}.pk_dt b ON a.ts = b.ts ORDER BY a.ts"),
    ("xprec-t92", "SELECT a.val FROM {L_ms}.t a, {M}.pk_dt b WHERE a.ts = b.ts AND a.ts < NOW() ORDER BY a.val"),
    ("xprec-t93", "SELECT TO_ISO8601(ts, '+00:00'), val FROM {L_ms}.t UNION ALL SELECT TO_ISO8601(ts, '+00:00'), val FROM {M}.pk_dt ORDER BY 1"),
    # ── Local-only but TZ-dependent (raw TIMESTAMP output) ──
    ("xprec-34", "SELECT ts, val FROM {L_ms}.t UNION SELECT ts, val FROM {L_us}.t ORDER BY 1"),
    ("xprec-61", "SELECT CASE WHEN a.val > 3 THEN (SELECT MAX(time) FROM {I}.sensor) ELSE (SELECT MIN(ts) FROM {L_ms}.t) END FROM {L_ms}.t a ORDER BY a.ts"),
]

# ── XPCOND group (cross-precision conditional functions) ──
# TZ-independent cases only (no MySQL DATETIME involvement).
_XPCOND_CASES = [
    ("xpcond-02", "SELECT IF(a.val>3, a.ts, b.time) FROM {L_ms}.t a INNER JOIN {I}.sensor b ON a.ts = b.time ORDER BY a.ts"),
    ("xpcond-05", "SELECT NVL2(a.ts, a.ts, b.time) FROM {L_ms}.t a LEFT JOIN {I}.sensor b ON a.ts = b.time ORDER BY a.ts"),
    # xpcond-07/08 moved to _XPCOND_TZ_CASES (outputs raw TIMESTAMP — TZ-dependent)
    ("xpcond-10", "SELECT GREATEST(a.ts, b.time) FROM {L_ms}.t a INNER JOIN {I}.sensor b ON a.ts = b.time ORDER BY a.ts"),
    # ── Local cross-precision conditional ──
    ("xpcond-13", "SELECT IF(a.val>3, a.ts, b.ts) FROM {L_ms}.t a INNER JOIN {L_us}.t b ON a.ts = b.ts ORDER BY a.ts"),
    ("xpcond-14", "SELECT GREATEST(a.ts, b.ts) FROM {L_us}.t a INNER JOIN {L_ns}.t b ON a.ts = b.ts ORDER BY a.ts"),
    ("xpcond-15", "SELECT LEAST(a.ts, b.ts) FROM {L_ms}.t a INNER JOIN {L_ns}.t b ON a.ts = b.ts ORDER BY a.ts"),
    ("xpcond-16", "SELECT COALESCE(NULL, b.ts) FROM {L_ms}.t a LEFT JOIN {L_us}.t b ON a.ts = b.ts ORDER BY a.ts"),
    ("xpcond-17", "SELECT NULLIF(a.ts, b.ts) FROM {L_ms}.t a INNER JOIN {L_us}.t b ON a.ts = b.ts ORDER BY a.ts"),
    ("xpcond-18", "SELECT IFNULL(NULL, b.ts), NVL(NULL, a.ts) FROM {L_us}.t a INNER JOIN {L_ns}.t b ON a.ts = b.ts ORDER BY a.ts"),
    ("xpcond-19", "SELECT NVL2(a.ts, a.ts, b.time) FROM {L_us}.t a LEFT JOIN {I}.sensor b ON a.ts = b.time ORDER BY a.ts"),
]

# ── XPCOND-TZ group (TZ-sensitive: involve MySQL pk_dt DATETIME) ──
_XPCOND_TZ_CASES = [
    ("xpcond-01", "SELECT IF(a.val>3, a.ts, b.ts) FROM {L_ms}.t a INNER JOIN {M}.pk_dt b ON a.ts = b.ts ORDER BY a.ts"),
    ("xpcond-03", "SELECT IF(a.val>3, a.ts, b.time) FROM {M}.pk_dt a INNER JOIN {I}.sensor b ON a.ts = b.time ORDER BY a.ts"),
    ("xpcond-04", "SELECT IFNULL(NULL, b.ts), NVL(NULL, a.ts) FROM {L_ms}.t a INNER JOIN {M}.pk_dt b ON a.ts = b.ts ORDER BY a.ts"),
    ("xpcond-06", "SELECT NULLIF(a.ts, b.ts) FROM {L_ms}.t a INNER JOIN {M}.pk_dt b ON a.ts = b.ts ORDER BY a.ts"),
    ("xpcond-09", "SELECT GREATEST(a.ts, b.ts) FROM {L_ms}.t a INNER JOIN {M}.pk_dt b ON a.ts = b.ts ORDER BY a.ts"),
    ("xpcond-11", "SELECT LEAST(a.ts, b.time) FROM {M}.pk_dt a INNER JOIN {I}.sensor b ON a.ts = b.time ORDER BY a.ts"),
    ("xpcond-12", "SELECT LEAST(a.ts, b.ts, c.time) FROM {L_ms}.t a INNER JOIN {M}.pk_dt b ON a.ts = b.ts INNER JOIN {I}.sensor c ON a.ts = c.time ORDER BY a.ts"),
    # ── Local-only but TZ-dependent (raw TIMESTAMP output) ──
    ("xpcond-07", "SELECT COALESCE(NULL, b.time) FROM {L_ms}.t a LEFT JOIN {I}.sensor b ON a.ts = b.time ORDER BY a.ts"),
    ("xpcond-08", "SELECT COALESCE(NULL, NULL, b.time) FROM {L_ms}.t a LEFT JOIN {I}.sensor b ON a.ts = b.time ORDER BY a.ts"),
]

# ── TFUNC group (time functions, parity: local vs ext) ──
# {tbl} placeholder replaced by both local and external table
_TFUNC_PARITY_CASES = [
    ("tfunc-02", "SELECT TO_ISO8601(ts, '+00:00') FROM {tbl} ORDER BY ts"),
    ("tfunc-03", "SELECT TO_ISO8601(ts, '+08:00') FROM {tbl} ORDER BY ts"),
    ("tfunc-04", "SELECT CAST(ts AS BIGINT) FROM {tbl} ORDER BY ts"),
    ("tfunc-05", "SELECT CAST(1704067200000 AS TIMESTAMP) FROM {tbl} LIMIT 1"),
    ("tfunc-06", "SELECT TIMETRUNCATE(ts, 1m) FROM {tbl} ORDER BY ts"),
    ("tfunc-07", "SELECT TIMETRUNCATE(ts, 1h) FROM {tbl} ORDER BY ts"),
    ("tfunc-09", "SELECT TIMEDIFF(ts, '2024-01-01') FROM {tbl} ORDER BY ts"),
    ("tfunc-10", "SELECT TIMEDIFF(ts, '2024-01-01 00:00:00', 1s) FROM {tbl} ORDER BY ts"),
    ("tfunc-11", "SELECT TO_TIMESTAMP('2024-01-01 00:00:00', '%Y-%m-%d %H:%M:%S') FROM {tbl} LIMIT 1"),
    ("tfunc-14", "SELECT WEEKDAY(ts) FROM {tbl} ORDER BY ts"),
    ("tfunc-15", "SELECT DAYOFWEEK(ts) FROM {tbl} ORDER BY ts"),
    ("tfunc-16", "SELECT WEEK(ts) FROM {tbl} ORDER BY ts"),
    ("tfunc-17", "SELECT WEEKOFYEAR(ts) FROM {tbl} ORDER BY ts"),
    ("tfunc-18", "SELECT ELAPSED(ts) FROM {tbl}"),
    ("tfunc-19", "SELECT ELAPSED(ts, 1s) FROM {tbl}"),
    ("tfunc-20", "SELECT SPREAD(ts) FROM {tbl}"),
    ("tfunc-21", "SELECT FIRST(ts) FROM {tbl}"),
    ("tfunc-22", "SELECT LAST(ts) FROM {tbl}"),
    ("tfunc-24", "SELECT NOW() FROM {tbl} LIMIT 1", {"non_deterministic": True}),
    ("tfunc-25", "SELECT TODAY() FROM {tbl} LIMIT 1", {"non_deterministic": True}),
    ("tfunc-26", "SELECT TO_UNIXTIMESTAMP('2024-01-01 00:00:00') FROM {tbl} LIMIT 1"),
    ("tfunc-27", "SELECT TIMEDIFF(MAX(ts), MIN(ts), 1s) FROM {tbl}"),
    ("tfunc-28", "SELECT TIMETRUNCATE(ts, 1m) FROM {tbl} WHERE ts > '2024-01-01 00:01:00' ORDER BY ts"),
    ("tfunc-30", "SELECT DIFF(ts) FROM {tbl}"),
    ("tfunc-31", "SELECT DIFF(val) FROM {tbl}"),
    ("tfunc-32", "SELECT DERIVATIVE(val, 1s, 0) FROM {tbl}"),
    ("tfunc-33", "SELECT IRATE(val) FROM {tbl}"),
    ("tfunc-34", "SELECT TWA(val) FROM {tbl}"),
    ("tfunc-35", "SELECT STATEDURATION(val, 'GT', 2, 1s) FROM {tbl}"),
    ("tfunc-36", "SELECT STATECOUNT(val, 'GT', 2) FROM {tbl}"),
    ("tfunc-37", "SELECT MIN(ts) FROM {tbl}"),
    ("tfunc-38", "SELECT MAX(ts) FROM {tbl}"),
    ("tfunc-39", "SELECT LAG(ts, 1) FROM {tbl}"),
    ("tfunc-40", "SELECT LEAD(ts, 1) FROM {tbl}"),
    ("tfunc-41", "SELECT GREATEST(ts, '2024-01-01 00:02:00') FROM {tbl} ORDER BY ts"),
    ("tfunc-42", "SELECT LEAST(ts, '2024-01-01 00:02:00') FROM {tbl} ORDER BY ts"),
    ("tfunc-43", "SELECT SAMPLE(val, 2) FROM {tbl}", {"validate_in": {1, 2, 3, 4, 5}, "non_deterministic": True}),
    ("tfunc-44", "SELECT TAIL(val, 2) FROM {tbl}"),
    ("tfunc-45", "SELECT UNIQUE(val) FROM {tbl}", {"ordered": False}),
    ("tfunc-46", "SELECT INTERP(val) FROM {tbl} RANGE('2024-01-01', '2024-01-01 00:05:00') EVERY(2m) FILL(LINEAR)"),
    ("tfunc-47", "SELECT TIMETRUNCATE(ts, 1s) FROM {tbl} ORDER BY ts"),
    ("tfunc-48", "SELECT WEEK(ts, 1) FROM {tbl} ORDER BY ts"),
    ("tfunc-50", "SELECT CAST(CAST(ts AS BIGINT) AS TIMESTAMP) FROM {tbl} ORDER BY ts"),
    # ── Subquery precision: verify precision passes correctly through subqueries ──
    ("tfunc-51", "SELECT CAST(ts AS BIGINT) FROM (SELECT ts FROM {tbl} ORDER BY ts)"),
    ("tfunc-53", "SELECT TIMETRUNCATE(ts, 1s) FROM (SELECT ts FROM {tbl} ORDER BY ts)"),
    ("tfunc-54", "SELECT TIMEDIFF(ts, '2024-01-01') FROM (SELECT ts FROM {tbl} ORDER BY ts)"),
    ("tfunc-55", "SELECT NOW() FROM (SELECT ts FROM {tbl} LIMIT 1)", {"non_deterministic": True}),
    ("tfunc-56", "SELECT CAST(NOW() AS BIGINT) FROM (SELECT ts FROM {tbl} LIMIT 1)", {"non_deterministic": True}),
    ("tfunc-57", "SELECT TODAY() FROM (SELECT ts FROM {tbl} LIMIT 1)", {"non_deterministic": True}),
    ("tfunc-58", "SELECT CAST(ts AS BIGINT) FROM (SELECT ts FROM {tbl} WHERE val > 2 ORDER BY ts)"),
    ("tfunc-59", "SELECT ELAPSED(ts) FROM (SELECT ts, val FROM {tbl})"),
    ("tfunc-60", "SELECT SPREAD(ts) FROM (SELECT ts, val FROM {tbl})"),
]

# ── TPC group (pseudo-columns in windows, parity) ──
_TPC_PARITY_CASES = [
    ("tpc-01", "SELECT _wstart, COUNT(*) FROM {tbl} INTERVAL(1m) ORDER BY _wstart"),
    ("tpc-02", "SELECT _wend, COUNT(*) FROM {tbl} INTERVAL(1m) ORDER BY _wend"),
    ("tpc-03", "SELECT _wduration, COUNT(*) FROM {tbl} INTERVAL(1m)"),
    ("tpc-04", "SELECT _wstart, COUNT(*) FROM {tbl} SESSION(ts, 30s)"),
    ("tpc-05", "SELECT _wstart, _wend, COUNT(*) FROM {tbl} STATE_WINDOW(val)"),
    ("tpc-06", "SELECT _wstart, COUNT(*) FROM {tbl} EVENT_WINDOW START WITH val > 1 END WITH val >= 5"),
    ("tpc-07", "SELECT _wstart, COUNT(*) FROM {tbl} COUNT_WINDOW(2)"),
    ("tpc-08", "SELECT _wstart, COUNT(*) FROM {tbl} WHERE ts >= '2024-01-01 00:00:00' AND ts < '2024-01-01 00:05:00' INTERVAL(1m) FILL(VALUE, 0) ORDER BY _wstart"),
    ("tpc-09", "SELECT _qstart, _qend, COUNT(*) FROM {tbl} WHERE ts >= '2024-01-01' AND ts < '2024-01-01 00:05:00'"),
    ("tpc-10", "SELECT _qduration, COUNT(*) FROM {tbl} WHERE ts >= '2024-01-01' AND ts < '2024-01-01 00:05:00'"),
    ("tpc-11", "SELECT _wstart, COUNT(*) FROM {tbl} INTERVAL(2m) SLIDING(1m) ORDER BY _wstart"),
    ("tpc-12", "SELECT _wstart, val, COUNT(*) FROM {tbl} PARTITION BY val INTERVAL(5m)"),
    ("tpc-13", "SELECT CAST(_wstart AS BIGINT), COUNT(*) FROM {tbl} INTERVAL(1m) ORDER BY _wstart"),
    ("tpc-14", "SELECT _wstart, _wend, COUNT(*) FROM {tbl} EXTERNAL_WINDOW ((SELECT _wstart, _wend FROM {tbl} INTERVAL(1m)) w) ORDER BY _wstart"),
    ("tpc-15", "SELECT _wstart, val, COUNT(*) FROM {tbl} PARTITION BY val SESSION(ts, 30s)"),
    ("tpc-16", "SELECT _wstart, val, COUNT(*) FROM {tbl} PARTITION BY val EVENT_WINDOW START WITH val > 1 END WITH val >= 3"),
    ("tpc-17", "SELECT _wstart, COUNT(*), FIRST(val) FROM {tbl} WHERE ts >= '2024-01-01 00:00:00' AND ts < '2024-01-01 00:05:00' INTERVAL(1m) FILL(PREV) ORDER BY _wstart"),
    ("tpc-18", "SELECT _wstart, AVG(val) FROM {tbl} WHERE ts >= '2024-01-01 00:00:00' AND ts < '2024-01-01 00:05:00' INTERVAL(1m) FILL(LINEAR) ORDER BY _wstart"),
    ("tpc-19", "SELECT _wstart, val, COUNT(*) FROM {tbl} PARTITION BY val COUNT_WINDOW(2)"),
    ("tpc-20", "SELECT _wstart, val, COUNT(*) FROM {tbl} PARTITION BY val STATE_WINDOW(val)"),
]

# ── TMULTI-TZ group (TZ-sensitive multi-timestamp cases; tested in test_timezone) ──
# All involve MySQL pk_dt (DATETIME) or PG pk_ts/multi_ts (TIMESTAMP WITHOUT TZ).
_TMULTI_TZ_CASES = [
    ("tmulti-01", "SELECT ts, ts_aware, ts_naive, ts_date FROM {M}.multi_ts ORDER BY ts"),
    ("tmulti-02", "SELECT CAST(ts AS BIGINT), CAST(ts_aware AS BIGINT), CAST(ts_naive AS BIGINT), CAST(ts_date AS BIGINT) FROM {M}.multi_ts ORDER BY ts"),
    ("tmulti-03", "SELECT ts, ts_aware, ts_naive, ts_date FROM {P}.multi_ts ORDER BY ts"),
    ("tmulti-04", "SELECT CAST(ts AS BIGINT), CAST(ts_aware AS BIGINT), CAST(ts_naive AS BIGINT), CAST(ts_date AS BIGINT) FROM {P}.multi_ts ORDER BY ts"),
    ("tmulti-05", "SELECT * FROM {M}.multi_ts WHERE ts_aware > ts ORDER BY ts"),
    ("tmulti-06", "SELECT * FROM {P}.multi_ts WHERE ts_aware > ts ORDER BY ts"),
    ("tmulti-07", "SELECT TIMEDIFF(ts_aware, ts, 1s) FROM {M}.multi_ts ORDER BY ts"),
    ("tmulti-08", "SELECT TIMEDIFF(ts_aware, ts, 1s) FROM {P}.multi_ts ORDER BY ts"),
    ("tmulti-09", "SELECT TO_ISO8601(ts), TO_ISO8601(ts_aware), TO_ISO8601(ts_naive) FROM {M}.multi_ts ORDER BY ts"),
    ("tmulti-10", "SELECT TO_ISO8601(ts), TO_ISO8601(ts_aware), TO_ISO8601(ts_naive) FROM {P}.multi_ts ORDER BY ts"),
    ("tmulti-11", "SELECT a.ts, b.ts FROM {M}.pk_dt a INNER JOIN {P}.pk_ts b ON a.ts = b.ts ORDER BY a.ts"),
    ("tmulti-12", "SELECT a.ts, b.time FROM {M}.pk_dt a INNER JOIN {I}.sensor b ON a.ts = b.time ORDER BY a.ts"),
    ("tmulti-13", "SELECT ts FROM {M}.pk_dt UNION ALL SELECT ts FROM {P}.pk_ts UNION ALL SELECT time FROM {I}.sensor ORDER BY 1"),
]

# ── TMULTI group (multi-timestamp columns — TZ-insensitive only) ──
# TZ-sensitive cases (tmulti-01–13) moved to _TMULTI_TZ_CASES.
_TMULTI_CASES = [
    ("tmulti-14", "SELECT a.ts, b.ts FROM {L_ms}.t a, {L_us}.t b WHERE a.ts = b.ts ORDER BY a.ts"),
    ("tmulti-15", "SELECT ts FROM {L_ms}.t UNION ALL SELECT ts FROM {L_us}.t UNION ALL SELECT ts FROM {L_ns}.t ORDER BY 1"),
]

# ── INS group (INSERT SELECT cross-precision) ──
_INS_CASES = [
    # (case_id, insert_sql, verify_sql, description)
    ("ins-01", "INSERT INTO {L_ms}.ins_target SELECT ts, val FROM {M}.pk_dt",
     "SELECT CAST(ts AS BIGINT), val FROM {L_ms}.ins_target ORDER BY ts",
     "MySQL(µs) → Local(ms) truncate"),
    ("ins-02", "INSERT INTO {L_ns}.ins_target SELECT ts, val FROM {M}.pk_dt",
     "SELECT CAST(ts AS BIGINT), val FROM {L_ns}.ins_target ORDER BY ts",
     "MySQL(µs) → Local(ns) expand"),
    ("ins-03", "INSERT INTO {L_ms}.ins_target SELECT time, val FROM {I}.sensor",
     "SELECT CAST(ts AS BIGINT), val FROM {L_ms}.ins_target ORDER BY ts",
     "InfluxDB(ns) → Local(ms) truncate"),
    ("ins-04", "INSERT INTO {L_us}.ins_target SELECT time, val FROM {I}.sensor",
     "SELECT CAST(ts AS BIGINT), val FROM {L_us}.ins_target ORDER BY ts",
     "InfluxDB(ns) → Local(µs) truncate"),
    ("ins-05", "INSERT INTO {L_us}.ins_target SELECT ts, val FROM {L_ms}.t",
     "SELECT CAST(ts AS BIGINT), val FROM {L_us}.ins_target ORDER BY ts",
     "Local(ms) → Local(µs) expand"),
    ("ins-06", "INSERT INTO {L_ns}.ins_target SELECT ts, val FROM {L_ms}.t",
     "SELECT CAST(ts AS BIGINT), val FROM {L_ns}.ins_target ORDER BY ts",
     "Local(ms) → Local(ns) expand"),
    ("ins-07", "INSERT INTO {L_ms}.ins_target SELECT ts, val FROM {L_ns}.t",
     "SELECT CAST(ts AS BIGINT), val FROM {L_ms}.ins_target ORDER BY ts",
     "Local(ns) → Local(ms) truncate"),
    ("ins-08", "INSERT INTO {L_ns}.ins_target SELECT ts, val FROM {L_us}.t",
     "SELECT CAST(ts AS BIGINT), val FROM {L_ns}.ins_target ORDER BY ts",
     "Local(µs) → Local(ns) expand"),
    ("ins-09", "INSERT INTO {L_ms}.ins_target SELECT ts, val FROM {P}.pk_ts",
     "SELECT CAST(ts AS BIGINT), val FROM {L_ms}.ins_target ORDER BY ts",
     "PG(µs) → Local(ms) truncate"),
]

# ── NEG group (time-parsing negative cases) ──
_NEG_CASES = [
    ("neg-01", "SELECT * FROM {M}.pk_dt WHERE ts > 'not-a-date'",
     TSDB_CODE_INVALID_TIMESTAMP),
    # Truncating ms-precision table to a sub-ms unit is intentionally rejected by
    # validateTimeUnitParam (builtins.c).  This is a designed boundary, not a bug.
    ("neg-05", "SELECT TIMETRUNCATE(ts, 1u) FROM {L_ms}.t ORDER BY ts",
     TSDB_CODE_FUNC_TIME_UNIT_TOO_SMALL),
]

# ── EDGE group (overflow/clamp/silent behaviors) ──
_EDGE_CASES = [
    ("edge-01", "SELECT CAST(9999999999999 AS TIMESTAMP) FROM {M}.pk_dt LIMIT 1"),
    ("edge-02", "SELECT CAST('garbage' AS TIMESTAMP) FROM {M}.pk_dt LIMIT 1"),
    ("edge-04", "SELECT * FROM {M}.pk_dt WHERE ts > 9999999999999999"),
    ("edge-05", "SELECT * FROM {L_ms}.t WHERE ts >= 0 ORDER BY ts"),
    ("edge-06", "SELECT * FROM {L_ms}.t WHERE ts > -1 ORDER BY ts"),
]


# =====================================================================
# Test class
# =====================================================================

class TestFq04TimezonePrecision(FederatedQueryTestMixin):
    """Timezone & precision full-coverage test for federated query.

    Groups:
        test_timezone            — tz: CST vs UTC, tz-aware vs no-tz
        test_time_format         — tfmt: WHERE string/epoch formats
        test_precision_single    — prec: single precision correctness
        test_precision_cross     — xprec+xpcond: cross-precision ops
        test_time_functions      — tfunc: time functions (parity)
        test_time_pseudocols     — tpc: pseudo-columns in windows (parity)
        test_multi_timestamp     — tmulti: multi-timestamp columns
        test_insert_select       — ins: INSERT SELECT cross-precision
        test_negative            — neg: time-parsing errors
        test_edge_cases          — edge: overflow/clamp/silent behaviors
    """

    updatecfgDict = {
        "federatedQueryEnable": 1,
        "clientCfg": {"federatedQueryEnable": 1},
    }

    _class_setup_done = False
    # Baseline files: source of truth.  NEVER regenerate by copying tmp output.
    # See BASELINE MAINTENANCE POLICY in module docstring.
    _ANS_DIR = os.path.join(os.path.dirname(__file__), "ans", "test_fq_04_timezone_precision")
    # Tmp files: for debugging diff details ONLY, NOT for regenerating baselines.
    _TMP_DIR = os.path.join(os.path.dirname(__file__), "tmp", "test_fq_04_timezone_precision")

    def _fmt(self, sql):
        """Replace placeholders in SQL template."""
        return sql.format(
            M=_SRC_M, P=_SRC_P, I=_SRC_I,
            L_ms=_LOCAL_DB_MS, L_us=_LOCAL_DB_US, L_ns=_LOCAL_DB_NS,
        )

    # ── Setup ──

    def setup_method(self, method):
        # Force the taos Python connector to format all naive timestamps in UTC
        # so that baseline comparisons are independent of the OS local timezone.
        # The taos.field module derives its epoch base from datetime.fromtimestamp(0),
        # which depends on OS TZ.  set_tz(UTC) switches to a fixed UTC epoch base,
        # making serialised datetime strings match the UTC-machine-generated baselines.
        _taos_field.set_tz(_pytz.UTC)

        if TestFq04TimezonePrecision._class_setup_done:
            return
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()
        ExtSrcEnv.ensure_env()
        self._setup_data()
        TestFq04TimezonePrecision._class_setup_done = True

    def _setup_data(self):
        tdLog.info("[fq04] Setting up test data ...")

        # Local TDengine databases
        for sql in _LOCAL_SETUP:
            tdSql.execute(sql)

        # MySQL
        m_cfg = self._mysql_cfg()
        ExtSrcEnv.mysql_kill_sleeping_connections_cfg(m_cfg)
        ExtSrcEnv.mysql_create_db_cfg(m_cfg, _MYSQL_DB)
        ExtSrcEnv.mysql_exec_cfg(m_cfg, _MYSQL_DB, _MYSQL_SETUP)
        self._cleanup_src(_SRC_M)
        self._mk_mysql_real(_SRC_M, database=_MYSQL_DB)

        # PostgreSQL
        p_cfg = self._pg_cfg()
        ExtSrcEnv.pg_create_db_cfg(p_cfg, _PG_DB)
        ExtSrcEnv.pg_exec_cfg(p_cfg, _PG_DB, _PG_SETUP)
        self._cleanup_src(_SRC_P)
        self._mk_pg_real(_SRC_P, database=_PG_DB)

        # InfluxDB
        i_cfg = self._influx_cfg()
        ExtSrcEnv.influx_write_cfg(i_cfg, _INFLUX_DB, _INFLUX_LINES)
        self._cleanup_src(_SRC_I)
        self._mk_influx_real(_SRC_I, database=_INFLUX_DB)

        tdLog.info("[fq04] Data setup complete.")

    def teardown_class(self):
        tmp = TestFq04TimezonePrecision()
        tmp._cleanup_src(_SRC_M, _SRC_P, _SRC_I)
        for db in [_LOCAL_DB_MS, _LOCAL_DB_US, _LOCAL_DB_NS]:
            tdSql.execute(f"DROP DATABASE IF EXISTS {db}")
        try:
            m_cfg = tmp._mysql_cfg()
            ExtSrcEnv.mysql_drop_db_cfg(m_cfg, _MYSQL_DB)
        except Exception:
            pass
        try:
            p_cfg = tmp._pg_cfg()
            ExtSrcEnv.pg_drop_db_cfg(p_cfg, _PG_DB)
        except Exception:
            pass
        TestFq04TimezonePrecision._class_setup_done = False
        ExtSrcEnv.teardown_env()

    def _run_parity_phase(self, cases, tz_label, local_tbl, ext_sources):
        """Run parity catalog by executing each source independently.

        Each case is executed once per source (LOCAL + external sources) and
        serialized as an independent block. This keeps tmp/baseline output
        truthful to the real query result of each source.
        """
        blocks, failed = [], []
        source_runs = [("LOCAL", local_tbl)] + list(ext_sources)

        for entry in cases:
            case_id   = entry[0]
            sql_tmpl  = entry[1]
            opts      = entry[2] if len(entry) > 2 else {}
            tagged_id = f"{tz_label}-{case_id}"

            for src_label, src_tbl in source_runs:
                src_case_id = self._source_case_id(tagged_id, src_label)
                passed, detail, serialized = self._run_source_case(
                    src_case_id,
                    sql_tmpl,
                    src_tbl,
                    validate_in=opts.get("validate_in"),
                    non_deterministic=opts.get("non_deterministic", False),
                    ordered=opts.get("ordered", True),
                )
                if serialized:
                    blocks.append(serialized)
                if not passed:
                    failed.append((src_case_id, sql_tmpl, detail))

        return blocks, failed

    def _source_case_id(self, case_id, src_label):
        """Attach source label to case id for per-source baseline blocks."""
        normalized = re.sub(r"[^a-zA-Z0-9]+", "_", str(src_label).strip().lower()).strip("_")
        if not normalized:
            normalized = "src"
        return f"{case_id}@{normalized}"

    def _run_source_case(self, case_id, sql_template, table_name, *,
                         validate_in=None,
                         non_deterministic=False,
                         ordered=True,
                         expect=None):
        """Execute one SQL against one concrete source table and serialize it."""
        prefix = f"[{case_id:<18s} POS]"
        sql = sql_template.format(tbl=table_name)
        t0 = _time.monotonic()

        try:
            tdSql.query(sql, queryTimes=1)
            rows = tdSql.queryResult or []

            if not ordered:
                rows = sorted(
                    rows,
                    key=lambda row: tuple("" if v is None else str(v) for v in row),
                )

            if validate_in is not None:
                for ri, row in enumerate(rows):
                    for ci, v in enumerate(row):
                        if v not in validate_in:
                            elapsed = _time.monotonic() - t0
                            msg = f"row[{ri}] col[{ci}]={v!r} not in {validate_in}"
                            tdLog.info(f"{prefix} FAIL  {msg}  [{elapsed:.2f}s]")
                            return False, msg, self._serialize_err(case_id, sql, msg)

                self._check_expect(case_id, rows, expect)
                elapsed = _time.monotonic() - t0
                tdLog.info(f"{prefix} PASS  {sql_template[:70]}  (validate_in)  [{elapsed:.2f}s]")
                return True, "", self._serialize_pos(
                    case_id, sql, rows, non_deterministic=non_deterministic)

            self._check_expect(case_id, rows, expect)
            elapsed = _time.monotonic() - t0
            tdLog.info(f"{prefix} PASS  {sql_template[:70]}  [{elapsed:.2f}s]")
            return True, "", self._serialize_pos(
                case_id, sql, rows, non_deterministic=non_deterministic)
        except Exception as e:
            elapsed = _time.monotonic() - t0
            msg = str(e)
            tdLog.info(f"{prefix} FAIL  {sql_template[:70]}  [{elapsed:.2f}s]  {msg}")
            return False, msg, self._serialize_err(case_id, sql, msg)

    # ── Baseline helpers ──

    def _baseline_path(self, name):
        return os.path.join(self._ANS_DIR, f"{name}.txt")

    def _run_baseline_group(self, cases, baseline_name, *, use_parity=False,
                            local_tbl=None, ext_sources=None):
        """Execute a list of cases and compare against baseline file.

        For non-parity groups: just run each SQL with placeholder substitution.
        For parity groups: run local + each ext source independently and serialize.
        """
        os.makedirs(self._ANS_DIR, exist_ok=True)
        baseline_file = self._baseline_path(baseline_name)
        blocks = []
        failed = []

        for entry in cases:
            case_id = entry[0]
            sql_template = entry[1]
            opts = entry[2] if len(entry) > 2 else {}
            validate_in = opts.get("validate_in")
            non_deterministic = opts.get("non_deterministic", False)
            ordered = opts.get("ordered", True)
            expect = opts.get("expect")

            if use_parity and local_tbl and ext_sources:
                # Source-truth mode for parity catalog: execute and serialize each source.
                source_runs = [("LOCAL", local_tbl)] + list(ext_sources)
                for src_label, src_tbl in source_runs:
                    src_case_id = self._source_case_id(case_id, src_label)
                    passed, detail, serialized = self._run_source_case(
                        src_case_id,
                        sql_template,
                        src_tbl,
                        validate_in=validate_in,
                        non_deterministic=non_deterministic,
                        ordered=ordered,
                        expect=expect,
                    )
                    if serialized:
                        blocks.append(serialized)
                    if not passed:
                        failed.append((src_case_id, sql_template, detail))
            else:
                # Baseline-only mode
                t0 = _time.monotonic()
                sql = self._fmt(sql_template)
                try:
                    tdSql.query(sql, queryTimes=1)
                    rows = tdSql.queryResult or []
                    if not ordered:
                        rows = sorted(
                            rows,
                            key=lambda row: tuple("" if v is None else str(v) for v in row),
                        )
                    self._check_expect(case_id, rows, expect)
                    serialized = self._serialize_pos(case_id, sql_template, rows,
                                                     non_deterministic=non_deterministic)
                    blocks.append(serialized)
                    elapsed = _time.monotonic() - t0
                    tdLog.info(f"[{case_id:<10s} POS] PASS  {sql_template[:70]}  [{elapsed:.2f}s]")
                except Exception as e:
                    elapsed = _time.monotonic() - t0
                    tdLog.info(f"[{case_id:<10s} POS] ERR   {sql_template[:70]}  [{elapsed:.2f}s]  {e}")
                    blocks.append(self._serialize_err(case_id, sql_template, str(e)))

        # Baseline comparison
        output = "\n".join(blocks) + "\n"
        self._compare_baseline(output, baseline_file, failed)
        return failed

    def _run_dual_tz_baseline_group(self, cases, baseline_name):
        """Run all cases under CST then UTC, compare against a single baseline.

        Each case ID is prefixed with 'cst-' or 'utc-' in the baseline output.
        This ensures every case is tested under two explicit client timezones,
        making results independent of the OS timezone.
        """
        os.makedirs(self._ANS_DIR, exist_ok=True)
        baseline_file = self._baseline_path(baseline_name)
        blocks = []
        failed = []

        for tz_label, tz_value in [("cst", "Asia/Shanghai"), ("utc", "UTC")]:
            tdSql.execute(f'ALTER LOCAL "timezone" "{tz_value}"')
            tdLog.info(f"[{baseline_name}] timezone → {tz_value}")
            _time.sleep(0.5)

            for entry in cases:
                case_id = entry[0]
                sql_template = entry[1]
                opts = entry[2] if len(entry) > 2 else {}
                non_deterministic = opts.get("non_deterministic", False)
                ordered = opts.get("ordered", True)
                expect = opts.get("expect")
                expect_error = opts.get("expect_error")
                tagged_id = f"{tz_label}-{case_id}"

                t0 = _time.monotonic()
                sql = self._fmt(sql_template)
                try:
                    tdSql.query(sql, queryTimes=1)
                    rows = tdSql.queryResult or []
                    if expect_error is not None:
                        elapsed = _time.monotonic() - t0
                        msg = f"expected error containing '{expect_error}', but query succeeded"
                        tdLog.info(f"[{tagged_id:<20s} NEG] FAIL  {sql_template[:70]}  [{elapsed:.2f}s]  {msg}")
                        blocks.append(self._serialize_pos(tagged_id, sql_template, rows,
                                                          non_deterministic=non_deterministic))
                        failed.append((tagged_id, sql_template, msg))
                        continue
                    if not ordered:
                        rows = sorted(
                            rows,
                            key=lambda row: tuple("" if v is None else str(v) for v in row),
                        )
                    self._check_expect(tagged_id, rows, expect)
                    serialized = self._serialize_pos(tagged_id, sql_template, rows,
                                                     non_deterministic=non_deterministic)
                    blocks.append(serialized)
                    elapsed = _time.monotonic() - t0
                    tdLog.info(f"[{tagged_id:<20s} POS] PASS  {sql_template[:70]}  [{elapsed:.2f}s]")
                except Exception as e:
                    elapsed = _time.monotonic() - t0
                    err = str(e)
                    blocks.append(self._serialize_err(tagged_id, sql_template, err))
                    if expect_error is not None and expect_error in err:
                        tdLog.info(f"[{tagged_id:<20s} NEG] PASS  {sql_template[:70]}  [{elapsed:.2f}s]  {e}")
                    else:
                        tdLog.info(f"[{tagged_id:<20s} POS] FAIL  {sql_template[:70]}  [{elapsed:.2f}s]  {e}")
                        failed.append((tagged_id, sql_template, err))

        output = "\n".join(blocks) + "\n"
        self._compare_baseline(output, baseline_file, failed)
        return failed

    @staticmethod
    def _check_expect(case_id, rows, expect):
        """Assert *rows* matches *expect* — independent correctness check.

        *expect* is a list-of-lists.  Each inner list is one row of expected
        values.  Use ``_ANY`` for cells that should accept any value (e.g.
        non-deterministic columns like NOW()).  Matching is done after
        converting every cell to ``str`` (same serialisation used for
        baselines).
        """
        if expect is None:
            return  # no independent expectation — rely on baseline only
        actual = [
            [str(v) if v is not None else "NULL" for v in row]
            for row in rows
        ]
        expected = [
            ["<ANY>" if v is _ANY else (str(v) if v is not None else "NULL") for v in row]
            for row in expect
        ]
        if len(actual) != len(expected):
            raise AssertionError(
                f"[{case_id}] expect mismatch: row count {len(actual)} != {len(expected)}\n"
                f"  expected: {expected}\n"
                f"  actual:   {actual}"
            )
        for r_idx, (a_row, e_row) in enumerate(zip(actual, expected)):
            for c_idx, (a, e) in enumerate(zip(a_row, e_row)):
                if e == "<ANY>":
                    continue
                if a != e:
                    raise AssertionError(
                        f"[{case_id}] expect mismatch at row {r_idx} col {c_idx}:\n"
                        f"  expected: {expected}\n"
                        f"  actual:   {actual}"
                    )

    def _serialize_pos(self, case_id, sql_template, rows, non_deterministic=False):
        lines = [f"### {case_id} POS", f"SQL: {sql_template}", "RESULT"]
        for row in rows:
            if non_deterministic:
                lines.append("<NON_DETERMINISTIC>")
            else:
                cells = []
                for v in row:
                    if v is None:
                        cells.append("NULL")
                    elif isinstance(v, _dt_mod.datetime) and v.tzinfo is not None:
                        # Strip tz-offset from tz-aware datetimes produced by
                        # taos.field.set_tz() so output format stays compatible
                        # with the naive-datetime baseline format.
                        cells.append(str(v.replace(tzinfo=None)))
                    else:
                        cells.append(str(v))
                lines.append("|".join(cells))
        lines.append("---")
        return "\n".join(lines)

    def _serialize_err(self, case_id, sql_template, err):
        lines = [f"### {case_id} ERR", f"SQL: {sql_template}", f"ERROR: {err}", "---"]
        return "\n".join(lines)

    def _compare_baseline(self, output, baseline_file, failed):
        """Compare output against baseline file.

        Raises a baseline-mismatch entry in *failed* when content differs.
        If the baseline file does not exist, raises an entry immediately.

        == IMPORTANT ==
        Baseline files must NEVER be regenerated by copying tmp output to ans/.
        Tmp files exist ONLY for debugging diff details.  When the baseline
        needs updating, manually inspect the specific wrong entry and edit it
        by hand.  See BASELINE MAINTENANCE POLICY in the module docstring.
        """
        _norm_re = re.compile(r'(root@)[0-9a-f]{12}\b')
        def normalize(text):
            return _norm_re.sub(r'\1<HOST>', text)

        os.makedirs(self._TMP_DIR, exist_ok=True)
        tmp_file = os.path.join(
            self._TMP_DIR,
            f"{os.path.basename(baseline_file)}.{os.getpid()}.tmp",
        )
        with open(tmp_file, "w") as f:
            f.write(output)
        tdLog.info(f"Temp result: {tmp_file}")

        if not os.path.isfile(baseline_file):
            msg = (
                f"Baseline file not found: {baseline_file}\n"
                f"  Actual output saved to: {tmp_file}\n"
                f"  Baseline must be MANUALLY created — do NOT copy tmp to ans/.\n"
                f"  Inspect tmp output, verify correctness, then write baseline by hand."
            )
            tdLog.info(f"BASELINE MISSING: {msg}")
            failed.append(("<baseline>", "<baseline>", msg))
            return

        with open(baseline_file, "r") as f:
            baseline = f.read()
        if normalize(output) != normalize(baseline):
            out_lines = output.splitlines()
            base_lines = baseline.splitlines()
            diff_line = -1
            for li in range(max(len(out_lines), len(base_lines))):
                tl = out_lines[li] if li < len(out_lines) else "<EOF>"
                bl = base_lines[li] if li < len(base_lines) else "<EOF>"
                if tl != bl:
                    diff_line = li + 1
                    break
            msg = (
                f"Baseline mismatch!\n"
                f"  baseline: {baseline_file}\n"
                f"  actual:   {tmp_file}\n"
                f"  first diff at line {diff_line}:\n"
                f"    baseline: {bl!r}\n"
                f"    actual:   {tl!r}\n"
                f"  Run: diff {baseline_file} {tmp_file}"
            )
            tdLog.info(f"BASELINE MISMATCH: {msg}")
            failed.append(("<baseline>", "<baseline>", msg))
        else:
            tdLog.info(f"Baseline comparison: OK ({baseline_file})")
            # Baseline matched — all ERR entries are known/expected.
            # Clear individual case errors so only regressions cause failure.
            failed.clear()

    # ── Test methods ──

    def test_timezone(self):
        """Timezone behavior: CST vs UTC, tz-aware vs no-tz types.

        Runs all _TZ_CASES in two phases: first under Asia/Shanghai, then UTC.
        Results are serialized with cst-/utc- prefixes into one baseline file.

        Catalog: - Query:FederatedTimezonePrecision

        Since: v3.4.0.0

        Labels: common,ci
        """
        os.makedirs(self._ANS_DIR, exist_ok=True)
        baseline_file = self._baseline_path("test_timezone")
        blocks = []
        failed = []

        parity_local_tbl = f"{_LOCAL_DB_US}.t"

        for tz_label, tz_value in [("cst", "Asia/Shanghai"), ("utc", "UTC")]:
            tdSql.execute(f'ALTER LOCAL "timezone" "{tz_value}"')
            tdLog.info(f"[tz] Switched to timezone: {tz_value}")
            _time.sleep(0.5)

            # Choose parity source whose calendar strings match the current session TZ:
            #   CST session: pk_ts_parity (CST strings) → mktime_z(CST, CST_str) = correct UTC epoch
            #   UTC session: pk_ts      (UTC strings) → mktime_z(UTC, UTC_str) = correct UTC epoch
            if tz_label == "cst":
                parity_ext_sources = [("PG", f"{_SRC_P}.pk_ts_parity")]
            else:
                parity_ext_sources = [("PG", f"{_SRC_P}.pk_ts")]

            for case_id, sql_template in _TZ_CASES:
                tagged_id = f"{tz_label}-{case_id}"
                sql = self._fmt(sql_template)
                t0 = _time.monotonic()
                try:
                    tdSql.query(sql, queryTimes=1)
                    rows = tdSql.queryResult or []
                    blocks.append(self._serialize_pos(tagged_id, sql_template, rows))
                    elapsed = _time.monotonic() - t0
                    tdLog.info(f"[{tagged_id:<16s} POS] PASS  [{elapsed:.2f}s]")
                except Exception as e:
                    elapsed = _time.monotonic() - t0
                    tdLog.info(f"[{tagged_id:<16s} POS] FAIL  {e}  [{elapsed:.2f}s]")
                    failed.append((tagged_id, sql_template, str(e)))
                    blocks.append(self._serialize_err(tagged_id, sql_template, str(e)))

            # ── Time-function parity (formerly test_time_functions) ──
            tf_blocks, tf_failed = self._run_parity_phase(
                _TFUNC_PARITY_CASES, tz_label, parity_local_tbl, parity_ext_sources)
            blocks.extend(tf_blocks)
            failed.extend(tf_failed)

            # ── Pseudo-column parity (formerly test_time_pseudocols) ──
            tpc_blocks, tpc_failed = self._run_parity_phase(
                _TPC_PARITY_CASES, tz_label, parity_local_tbl, parity_ext_sources)
            blocks.extend(tpc_blocks)
            failed.extend(tpc_failed)

        output = "\n".join(blocks) + "\n"
        self._compare_baseline(output, baseline_file, failed)

        if failed:
            raise AssertionError(
                f"{len(failed)} tz case(s) failed:\n" +
                "\n".join(f"  [{cid}] {det}" for cid, _, det in failed)
            )

    def test_time_format(self):
        """WHERE clause time format variants: string, ms/µs/ns epoch, NOW(), TODAY().

        Catalog: - Query:FederatedTimezonePrecision

        Since: v3.4.0.0

        Labels: common,ci
        """
        all_cases = _TFMT_CASES + _TFMT_TZ_CASES
        failed = self._run_dual_tz_baseline_group(all_cases, "test_time_format")
        if failed:
            raise AssertionError(
                f"{len(failed)} tfmt case(s) failed:\n" +
                "\n".join(f"  [{cid}] {det}" for cid, _, det in failed)
            )

    def test_precision_single(self):
        """Single-precision correctness: verify CAST(ts AS BIGINT) returns correct epoch.

        Catalog: - Query:FederatedTimezonePrecision

        Since: v3.4.0.0

        Labels: common,ci
        """
        all_cases = _PREC_CASES + _PREC_TZ_CASES
        failed = self._run_dual_tz_baseline_group(all_cases, "test_precision_single")
        if failed:
            raise AssertionError(
                f"{len(failed)} prec case(s) failed:\n" +
                "\n".join(f"  [{cid}] {det}" for cid, _, det in failed)
            )

    def test_precision_cross(self):
        """Cross-precision operations: comparison, IN, UNION, JOIN, CASE WHEN, conditional funcs.

        Catalog: - Query:FederatedTimezonePrecision

        Since: v3.4.0.0

        Labels: common,ci
        """
        all_cases = _XPREC_CASES + _XPREC_TZ_CASES + _XPCOND_CASES + _XPCOND_TZ_CASES
        failed = self._run_dual_tz_baseline_group(all_cases, "test_precision_cross")
        if failed:
            raise AssertionError(
                f"{len(failed)} xprec/xpcond case(s) failed:\n" +
                "\n".join(f"  [{cid}] {det}" for cid, _, det in failed)
            )

    def test_multi_timestamp(self):
        """Multi-timestamp column queries across sources.

        Catalog: - Query:FederatedTimezonePrecision

        Since: v3.4.0.0

        Labels: common,ci
        """
        all_cases = _TMULTI_CASES + _TMULTI_TZ_CASES
        failed = self._run_dual_tz_baseline_group(all_cases, "test_multi_timestamp")
        if failed:
            raise AssertionError(
                f"{len(failed)} tmulti case(s) failed:\n" +
                "\n".join(f"  [{cid}] {det}" for cid, _, det in failed)
            )

    def test_insert_select(self):
        """INSERT SELECT cross-precision: verify truncation and expansion.

        Catalog: - Query:FederatedTimezonePrecision

        Since: v3.4.0.0

        Labels: common,ci
        """
        failed = []

        for tz_label, tz_value in [("cst", "Asia/Shanghai"), ("utc", "UTC")]:
            tdSql.execute(f'ALTER LOCAL "timezone" "{tz_value}"')
            tdLog.info(f"[ins] timezone → {tz_value}")
            _time.sleep(0.5)

            for case_id, insert_sql, verify_sql, desc in _INS_CASES:
                tagged_id = f"{tz_label}-{case_id}"
                t0 = _time.monotonic()
                ins = self._fmt(insert_sql)
                ver = self._fmt(verify_sql)

                # Determine target table for cleanup
                target_match = re.match(r'INSERT INTO\s+(\S+)\.ins_target', ins)
                if target_match:
                    target_db = target_match.group(1)
                    tdSql.execute(f"DELETE FROM {target_db}.ins_target")

                try:
                    tdSql.execute(ins)
                    tdSql.query(ver)
                    rows = tdSql.queryResult or []
                    if len(rows) != len(_ROWS):
                        raise AssertionError(
                            f"Expected {len(_ROWS)} rows, got {len(rows)}"
                        )
                    elapsed = _time.monotonic() - t0
                    tdLog.info(f"[{tagged_id:<12s} POS] PASS  {desc}  ({len(rows)} rows)  [{elapsed:.2f}s]")
                except Exception as e:
                    elapsed = _time.monotonic() - t0
                    tdLog.info(f"[{tagged_id:<12s} POS] FAIL  {desc}  [{elapsed:.2f}s]  {e}")
                    failed.append((tagged_id, insert_sql, str(e)))

        if failed:
            raise AssertionError(
                f"{len(failed)} ins case(s) failed:\n" +
                "\n".join(f"  [{cid}] {det}" for cid, _, det in failed)
            )

    def test_negative(self):
        """Negative cases: time-parsing errors return expected error codes.

        Catalog: - Query:FederatedTimezonePrecision

        Since: v3.4.0.0

        Labels: common,ci
        """
        failed = []

        for tz_label, tz_value in [("cst", "Asia/Shanghai"), ("utc", "UTC")]:
            tdSql.execute(f'ALTER LOCAL "timezone" "{tz_value}"')
            tdLog.info(f"[neg] timezone → {tz_value}")
            _time.sleep(0.5)

            for case_id, sql_template, expected_errno in _NEG_CASES:
                tagged_id = f"{tz_label}-{case_id}"
                sql = self._fmt(sql_template)
                t0 = _time.monotonic()
                try:
                    tdSql.query(sql, queryTimes=1)
                    if expected_errno is not None:
                        elapsed = _time.monotonic() - t0
                        msg = f"query succeeded, expected errno 0x{expected_errno & 0xFFFFFFFF:08X}"
                        tdLog.info(f"[{tagged_id:<12s} NEG] FAIL  {sql_template[:70]}  [{elapsed:.2f}s]  {msg}")
                        failed.append((tagged_id, sql_template, msg))
                    else:
                        elapsed = _time.monotonic() - t0
                        tdLog.info(f"[{tagged_id:<12s} NEG] PASS  {sql_template[:70]}  (succeeded as expected)  [{elapsed:.2f}s]")
                except Exception as e:
                    actual_errno = getattr(e, 'errno', None)
                    if expected_errno is not None and actual_errno is not None:
                        exp_lo = expected_errno & 0xFFFF
                        act_lo = actual_errno & 0xFFFF
                        if actual_errno == expected_errno or exp_lo == act_lo:
                            elapsed = _time.monotonic() - t0
                            tdLog.info(f"[{tagged_id:<12s} NEG] PASS  {sql_template[:70]}  [{elapsed:.2f}s]")
                        else:
                            elapsed = _time.monotonic() - t0
                            msg = f"errno {actual_errno} != expected {expected_errno}"
                            tdLog.info(f"[{tagged_id:<12s} NEG] FAIL  {sql_template[:70]}  [{elapsed:.2f}s]  {msg}")
                            failed.append((tagged_id, sql_template, msg))
                    elif expected_errno is None:
                        elapsed = _time.monotonic() - t0
                        msg = f"expected success but got error: {e}"
                        tdLog.info(f"[{tagged_id:<12s} NEG] FAIL  {sql_template[:70]}  [{elapsed:.2f}s]  {msg}")
                        failed.append((tagged_id, sql_template, msg))
                    else:
                        elapsed = _time.monotonic() - t0
                        tdLog.info(f"[{tagged_id:<12s} NEG] PASS  {sql_template[:70]}  (errored)  [{elapsed:.2f}s]")

        if failed:
            raise AssertionError(
                f"{len(failed)} neg case(s) failed:\n" +
                "\n".join(f"  [{cid}] {det}" for cid, _, det in failed)
            )

    def test_edge_cases(self):
        """Edge cases: overflow clamp, CAST garbage, sub-precision TIMETRUNCATE, extremes.

        Catalog: - Query:FederatedTimezonePrecision

        Since: v3.4.0.0

        Labels: common,ci
        """
        failed = self._run_dual_tz_baseline_group(_EDGE_CASES, "test_edge_cases")
        if failed:
            raise AssertionError(
                f"{len(failed)} edge case(s) failed:\n" +
                "\n".join(f"  [{cid}] {det}" for cid, _, det in failed)
            )
