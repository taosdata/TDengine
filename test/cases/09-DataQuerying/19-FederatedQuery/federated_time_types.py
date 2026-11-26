# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License
# for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.

"""Time-type catalog shared by the federated-query precision suites.

Both the direct (non-virtual) precision suite ``test_fq_04_timezone_precision``
and the virtual-table precision suite
``test_vtable_query_ext_source_timezone_precision`` have to enumerate the same
matrix - every time type, at every declared precision, for every supported
external source - and both have to compute the same expected epochs.  Keeping
one catalog here stops the two suites from drifting apart.

Two independent precision scopes are involved (Oracle DS §5.3.7):

* COLUMN precision (``extTimestampTypePrecision``) is derived from the
  declared type name.  It decides whether a virtual table may reference the
  column as its time axis and how a range literal is rendered.
* TRANSPORT precision (``SExtTableMeta.tsPrecision``) is the maximum over the
  table's time columns.  It decides how a fetched value is interpreted: the
  connector converts the value at this precision and the output block declares
  it.

A value read through a virtual table therefore loses nothing only when the
mapping handed to the connector carries the TRANSPORT precision, not the
column's own declaration.  ``expected_value()`` below applies the same two
truncations in that order, so a suite asserting against it catches any layer
that drops back to a coarser precision.

Catalog: - Query:FederatedTimezonePrecision
"""

import copy
import datetime as _dt
import os
import sys
import time
from zoneinfo import ZoneInfo

from new_test_framework.utils import tdLog

_HERE = os.path.dirname(os.path.abspath(__file__))
_DDL_EXT_DIR = os.path.abspath(
    os.path.join(_HERE, "..", "..", "05-VirtualTables", "01-DDL", "03-ExtSource")
)
if _DDL_EXT_DIR not in sys.path:
    sys.path.insert(0, _DDL_EXT_DIR)

from ext_source_helpers import (  # noqa: E402
    ExtSrcEnv,
    create_ext_source,
    create_influx_measurement,
    create_mysql_table,
    create_pg_table,
    create_remote_db,
    create_sqlserver_table,
)

# =====================================================================
# Precision model
# =====================================================================

RANK_INDEX = {"ms": 0, "us": 1, "ns": 2}
NS_PER_RANK = {"ms": 10 ** 6, "us": 10 ** 3, "ns": 1}
RANKS = ("ms", "us", "ns")


class TsType(object):
    """One external time type at one declared precision.

    label  - stable column-name suffix used by the suites
    decl   - the remote SQL declaration
    rank   - the COLUMN precision class (``extTimestampTypePrecision``)
    aware  - True when the type carries an absolute instant (tz-aware)
    frac   - fractional digits actually written into the column
    pk_ok  - False when the remote engine refuses this type as a primary key
    """

    __slots__ = ("label", "decl", "rank", "aware", "frac", "pk_ok",
                 "date_midnight")

    def __init__(self, label, decl, rank, aware, frac, pk_ok=True,
                 date_midnight=False):
        self.label = label
        self.decl = decl
        self.rank = rank
        self.aware = aware
        self.frac = frac
        self.pk_ok = pk_ok
        # A calendar DATE stores no time of day, so the fixture value the
        # engine returns is midnight, not the row's wall clock.  Oracle DATE
        # is the exception: it carries hh:mm:ss, and the literal below keeps
        # them.
        self.date_midnight = date_midnight

    @property
    def col(self):
        return "c_" + self.label

    def __repr__(self):
        return "TsType(%s, %s, %s)" % (self.label, self.decl, self.rank)


def _ora_rank(digits):
    if digits <= 3:
        return "ms"
    return "us" if digits <= 6 else "ns"


def _frac_rank(digits):
    """MySQL / PostgreSQL / SQL Server: <= 3 digits is millisecond aligned."""
    return "ms" if digits <= 3 else "us"


# MySQL: DATE, DATETIME(p), TIMESTAMP(p) (tz-aware).  TIME maps to BIGINT and
# is therefore not a timestamp type (DS §5.3.2), so it is out of this catalog.
MYSQL_TYPES = (
    [TsType("date", "DATE", "ms", False, 0, date_midnight=True)]
    + [TsType("datetime%d" % p, "DATETIME(%d)" % p, _frac_rank(p), False, p)
       for p in range(7)]
    + [TsType("datetime", "DATETIME", "ms", False, 0)]
    + [TsType("timestamp%d" % p, "TIMESTAMP(%d)" % p, _frac_rank(p), True, p)
       for p in range(7)]
    + [TsType("timestamp", "TIMESTAMP", "ms", True, 0)]
)

# PostgreSQL: DATE, TIMESTAMP(p) (naive), TIMESTAMPTZ(p) (aware).  A bare
# TIMESTAMP defaults to microsecond in extTimestampTypePrecision.
#
# DATE is microsecond-class here, unlike MySQL / SQL Server / Oracle: the
# PostgreSQL branch of extTimestampTypePrecision() only derives a precision
# for the "timestamp*" family, so DATE falls back to the table transport
# precision (microsecond).  Encoding that keeps the expected rejection matrix
# identical to what CREATE VTABLE really enforces.
PG_TYPES = (
    [TsType("date", "DATE", "us", False, 0, date_midnight=True)]
    + [TsType("timestamp%d" % p, "TIMESTAMP(%d)" % p, _frac_rank(p), False, p)
       for p in range(7)]
    + [TsType("timestamp", "TIMESTAMP", "us", False, 6)]
    + [TsType("timestamptz%d" % p, "TIMESTAMPTZ(%d)" % p, _frac_rank(p), True, p)
       for p in range(7)]
    + [TsType("timestamptz", "TIMESTAMPTZ", "us", True, 6)]
)

# SQL Server: DATE, SMALLDATETIME, DATETIME, DATETIME2(p), DATETIMEOFFSET(p).
# DATETIME carries 1/300 s ticks, so the fixture writes whole seconds only.
SQLSERVER_TYPES = (
    [TsType("date", "date", "ms", False, 0, date_midnight=True),
     TsType("smalldatetime", "smalldatetime", "ms", False, 0),
     TsType("datetime", "datetime", "us", False, 0)]
    + [TsType("datetime2_%d" % p, "datetime2(%d)" % p, _frac_rank(p), False, p)
       for p in range(8)]
    + [TsType("datetime2", "datetime2", "us", False, 7)]
    + [TsType("datetimeoffset_%d" % p, "datetimeoffset(%d)" % p, _frac_rank(p), True, p)
       for p in range(8)]
    + [TsType("datetimeoffset", "datetimeoffset", "us", True, 7)]
)

# Oracle: DATE, TIMESTAMP(p), TIMESTAMP(p) WITH [LOCAL] TIME ZONE.
# "TIMESTAMP WITH TIME ZONE" cannot be a primary key (ORA-02329), but it is a
# perfectly legal non-key column.
ORACLE_TYPES = (
    [TsType("date", "DATE", "ms", False, 0)]
    + [TsType("timestamp%d" % p, "TIMESTAMP(%d)" % p, _ora_rank(p), False, p)
       for p in range(10)]
    + [TsType("timestamp", "TIMESTAMP", "us", False, 6)]
    + [TsType("tstz%d" % p, "TIMESTAMP(%d) WITH TIME ZONE" % p, _ora_rank(p),
              True, p, False) for p in range(10)]
    + [TsType("tstz", "TIMESTAMP WITH TIME ZONE", "us", True, 6, False)]
    + [TsType("tstltz%d" % p, "TIMESTAMP(%d) WITH LOCAL TIME ZONE" % p,
              _ora_rank(p), True, p) for p in range(10)]
    + [TsType("tstltz", "TIMESTAMP WITH LOCAL TIME ZONE", "us", True, 6)]
)

# InfluxDB v3: a single time column, fixed UTC nanosecond epoch.
INFLUX_TYPES = [TsType("time", "time", "ns", True, 9)]

TYPES = {
    "mysql": MYSQL_TYPES,
    "pg": PG_TYPES,
    "sqlserver": SQLSERVER_TYPES,
    "oracle": ORACLE_TYPES,
    "influx": INFLUX_TYPES,
}


# =====================================================================
# Fixture rows and expected epochs
# =====================================================================

AWARE_OFFSET_HOURS = 8


class Row(object):
    """One fixture row: a wall clock, its fractional digits and a payload."""

    __slots__ = ("dt", "ns_digits", "v")

    def __init__(self, dt, ns_digits, v):
        self.dt = dt
        self.ns_digits = ns_digits
        self.v = v


# Seconds are 00 on purpose: SMALLDATETIME has minute granularity and would
# round anything else, which would make its expected value ambiguous.
ROWS = (
    Row(_dt.datetime(2024, 1, 1, 0, 2, 0), "123456789", 1),
    Row(_dt.datetime(2024, 1, 2, 3, 4, 0), "987654321", 2),
)


def _fraction(row, digits):
    if digits <= 0:
        return ""
    return "." + row.ns_digits[:digits]


def naive_wall(row, ts_type):
    """Wall-clock text for a tz-naive column (interpreted in the client tz)."""
    return row.dt.strftime("%Y-%m-%d %H:%M:%S") + _fraction(row, ts_type.frac)


def aware_wall(row, ts_type):
    """Wall-clock text carrying AWARE_OFFSET_HOURS, i.e. a fixed instant."""
    shifted = row.dt + _dt.timedelta(hours=AWARE_OFFSET_HOURS)
    return shifted.strftime("%Y-%m-%d %H:%M:%S") + _fraction(row, ts_type.frac)


def aware_offset_text():
    return "%+03d:00" % AWARE_OFFSET_HOURS


def row_epoch_ns(row, ts_type, tz_name):
    """Exact epoch the row's value represents, in nanoseconds.

    tz-aware columns pin the instant regardless of the client timezone;
    tz-naive columns are interpreted in the client timezone, exactly as the
    connector does.
    """
    digits = ts_type.frac
    frac_ns = (int((row.ns_digits + "000000000")[:digits]) * 10 ** (9 - digits)
               if digits > 0 else 0)
    wall = row.dt
    if ts_type.date_midnight:
        wall = wall.replace(hour=0, minute=0, second=0)
    if ts_type.aware:
        base = wall.replace(tzinfo=_dt.timezone.utc)
    else:
        base = wall.replace(tzinfo=ZoneInfo(tz_name))
    return int(base.timestamp()) * 10 ** 9 + frac_ns


def truncate_ns(value_ns, rank):
    unit = NS_PER_RANK[rank]
    return (value_ns // unit) * unit


def expected_value(row, ts_type, tz_name, transport_rank, dst_rank):
    """Expected ``CAST(col AS BIGINT)`` in ``dst_rank`` units.

    The value can never be finer than the column's declared precision, the
    connector converts it at the table TRANSPORT precision, and the virtual
    table then scales it to its own precision.  Applying the truncations in
    that order is what the engine must reproduce.
    """
    value_ns = row_epoch_ns(row, ts_type, tz_name)
    value_ns = truncate_ns(value_ns, ts_type.rank)
    value_ns = truncate_ns(value_ns, transport_rank)
    return value_ns // NS_PER_RANK[dst_rank]


def expected_direct_value(row, ts_type, tz_name, transport_rank):
    """Expected value of a DIRECT external-table read (source precision)."""
    value_ns = row_epoch_ns(row, ts_type, tz_name)
    value_ns = truncate_ns(value_ns, ts_type.rank)
    value_ns = truncate_ns(value_ns, transport_rank)
    return value_ns // NS_PER_RANK[transport_rank]


def table_transport_rank(types):
    """Generic transport precision: the maximum over the table's time columns.

    Use ``Backend.transport_rank()`` instead when the connector's own rule is
    coarser than that maximum - Oracle and SQL Server never transport below
    microsecond even for a DATE-only table, so a plain maximum would understate
    the unit a direct read returns.
    """
    if not types:
        return "ms"
    return max((t.rank for t in types), key=lambda rank: RANK_INDEX[rank])


# =====================================================================
# Backends
# =====================================================================

class Backend(object):
    """Provisioning + reference building for one external source kind.

    ``prepare()`` creates the remote database and the EXTERNAL SOURCE object;
    ``create_axis_table()`` creates a table whose primary key is one time
    type; ``create_mixed_table()`` creates a table whose primary key is one
    time type and whose remaining columns cover the whole catalog.
    """

    key = ""
    src_type = ""
    source = ""
    database = ""
    schema = None
    v_decl = "bigint"
    v_remote_decl = "BIGINT"

    # --- remote helpers -------------------------------------------------
    def table_ref(self, table):
        raise NotImplementedError

    def col_ref(self, table, col):
        return "%s.%s" % (self.table_ref(table), col)

    def direct_table_ref(self, table):
        """Reference used by a DIRECT external-table query (no column).

        Defaults to the same path as the column reference; PostgreSQL needs
        the short source.table form, because a four-segment reference is
        rendered with the database in the schema slot there.
        """
        return self.table_ref(table)

    def value_literal(self, ts_type, row):
        raise NotImplementedError

    def _col_def_list(self, pk_type, extra_types):
        defs = ["ts %s NOT NULL PRIMARY KEY" % pk_type.decl]
        defs += ["%s %s" % (t.col, t.decl) for t in extra_types]
        defs.append("v %s" % self.v_remote_decl)
        return defs

    def _col_defs(self, pk_type, extra_types):
        return ", ".join(self._col_def_list(pk_type, extra_types))

    def _row_values(self, pk_type, extra_types, row):
        values = [self.value_literal(pk_type, row)]
        values += [self.value_literal(t, row) for t in extra_types]
        values.append(str(row.v))
        return values

    def _row_literals(self, pk_type, extra_types):
        return ["(" + ", ".join(self._row_values(pk_type, extra_types, row)) + ")"
                for row in ROWS]

    def create_axis_table(self, table, pk_type):
        self.create_table(table, pk_type, [])

    def create_mixed_table(self, table, pk_type, extra_types):
        self.create_table(table, pk_type, extra_types)

    def create_table(self, table, pk_type, extra_types):
        raise NotImplementedError

    def prepare(self):
        create_remote_db(self.src_type, self.database)
        create_ext_source(self.source, self.src_type, self.database,
                          schema=self.schema)

    def transport_rank(self, types):
        """Transport precision the connector really uses for this table.

        Oracle / SQL Server decode at microsecond unless an Oracle table holds
        a 7..9 digit column; MySQL / PostgreSQL are always microsecond.  The
        value is never finer than a column's own declared precision, which is
        why ``expected_value()`` truncates at the column rank first.
        """
        return "us"

    def clone(self, source, database=None):
        """Copy with a different source name (and database).

        Suites that run side by side must not share an EXTERNAL SOURCE
        object; the Oracle database is the service name and stays fixed.
        """
        dup = copy.copy(self)
        dup.source = source
        if database is not None:
            dup.database = database
        return dup

    def cleanup(self, tables):
        """Best-effort removal of the fixtures this suite created."""
        raise NotImplementedError


class _MysqlBackend(Backend):
    key = "mysql"
    src_type = "mysql"
    source = "vt_ttp_my"
    database = "vt_ttp_mydb"

    def table_ref(self, table):
        return "%s.%s.%s" % (self.source, self.database, table)

    def value_literal(self, ts_type, row):
        if ts_type.label == "date":
            return "'%s'" % row.dt.strftime("%Y-%m-%d")
        if ts_type.aware:
            # FROM_UNIXTIME pins the instant; a bare literal would depend on
            # the server session time zone.
            epoch = int(row.dt.replace(tzinfo=_dt.timezone.utc).timestamp())
            return "FROM_UNIXTIME(%d%s)" % (epoch, _fraction(row, ts_type.frac))
        return "'%s'" % naive_wall(row, ts_type)

    def create_table(self, table, pk_type, extra_types):
        create_mysql_table(self.database, table,
                           self._col_defs(pk_type, extra_types),
                           self._row_literals(pk_type, extra_types))

    def cleanup(self, tables):
        for table in tables:
            try:
                ExtSrcEnv.mysql_exec(self.database,
                                     ["DROP TABLE IF EXISTS `%s`" % table])
            except Exception as exc:  # noqa: BLE001 - best-effort teardown
                tdLog.info("mysql cleanup %s: %s" % (table, exc))


class _PgBackend(Backend):
    key = "pg"
    src_type = "postgresql"
    source = "vt_ttp_pg"
    database = "vt_ttp_pgdb"
    schema = "public"

    def table_ref(self, table):
        return "%s.%s.%s" % (self.source, self.database, table)

    def direct_table_ref(self, table):
        return "%s.%s" % (self.source, table)

    def value_literal(self, ts_type, row):
        if ts_type.label == "date":
            return "'%s'" % row.dt.strftime("%Y-%m-%d")
        if ts_type.aware:
            return "'%s%s'" % (aware_wall(row, ts_type), aware_offset_text())
        return "'%s'" % naive_wall(row, ts_type)

    def create_table(self, table, pk_type, extra_types):
        create_pg_table(self.database, table,
                        self._col_defs(pk_type, extra_types),
                        self._row_literals(pk_type, extra_types))

    def cleanup(self, tables):
        stmts = ["DROP TABLE IF EXISTS %s" % table for table in tables]
        try:
            ExtSrcEnv.pg_exec(self.database, stmts)
        except Exception as exc:  # noqa: BLE001 - best-effort teardown
            tdLog.info("pg cleanup: %s" % exc)


class _SqlServerBackend(Backend):
    key = "sqlserver"
    src_type = "sqlserver"
    source = "vt_ttp_ss"
    database = "vt_ttp_ssdb"
    schema = "dbo"

    def table_ref(self, table):
        return "%s.%s.dbo.%s" % (self.source, self.database, table)

    def value_literal(self, ts_type, row):
        if ts_type.label == "date":
            return "'%s'" % row.dt.strftime("%Y-%m-%d")
        if ts_type.aware:
            return "'%s %s'" % (aware_wall(row, ts_type), aware_offset_text())
        return "'%s'" % naive_wall(row, ts_type)

    def create_table(self, table, pk_type, extra_types):
        create_sqlserver_table(self.database, table,
                               self._col_defs(pk_type, extra_types),
                               self._row_literals(pk_type, extra_types),
                               schema="dbo")

    def cleanup(self, tables):
        stmts = ["IF OBJECT_ID('dbo.%s') IS NOT NULL DROP TABLE dbo.%s"
                 % (table, table) for table in tables]
        try:
            ExtSrcEnv.sqlserver_exec(self.database, stmts)
        except Exception as exc:  # noqa: BLE001 - best-effort teardown
            tdLog.info("sqlserver cleanup: %s" % exc)


class _OracleBackend(Backend):
    key = "oracle"
    src_type = "oracle"
    source = "vt_ttp_or"
    schema = ExtSrcEnv.ORACLE_USER.upper()
    v_remote_decl = "NUMBER(10,0)"
    v_decl = "bigint"

    def __init__(self):
        self.database = next(ExtSrcEnv.oracle_version_configs()).service

    def transport_rank(self, types):
        return "ns" if any(t.rank == "ns" for t in types) else "us"

    def table_ref(self, table):
        # The schema segment is an identifier: TDengine lower-cases it, so the
        # upper-case Oracle account must be spelled between backticks.
        return "%s.`%s`.%s" % (self.source, self.schema, table)

    def _col_def_list(self, pk_type, extra_types):
        # The fixture stores quoted lower-case names; the shared lower-case
        # references then match the Oracle dictionary byte for byte.
        defs = ['"ts" %s NOT NULL PRIMARY KEY' % pk_type.decl]
        defs += ['"%s" %s' % (t.col, t.decl) for t in extra_types]
        defs.append('"v" %s' % self.v_remote_decl)
        return defs

    def value_literal(self, ts_type, row):
        if ts_type.label == "date":
            # Oracle DATE carries hh:mm:ss, so keep the wall clock instead of
            # collapsing it to midnight like a calendar DATE.
            return "TO_DATE('%s', 'YYYY-MM-DD HH24:MI:SS')" % naive_wall(
                row, ts_type)
        if ts_type.aware:
            return "TIMESTAMP '%s %s'" % (aware_wall(row, ts_type),
                                          aware_offset_text())
        return "TIMESTAMP '%s'" % naive_wall(row, ts_type)

    def create_table(self, table, pk_type, extra_types):
        # SQL*Plus ignores any input line longer than 2499 characters
        # (SP2-0027) while still exiting 0, so a 30+ column definition has to
        # be spread over several lines.  One column (value) per line keeps
        # every line far below the limit.
        cfg = next(ExtSrcEnv.oracle_version_configs())
        stmts = ['CREATE TABLE "%s" (\n  %s\n)' % (
            table, ",\n  ".join(self._col_def_list(pk_type, extra_types)))]
        for row in ROWS:
            stmts.append('INSERT INTO "%s" VALUES (\n  %s\n)' % (
                table, ",\n  ".join(self._row_values(pk_type, extra_types, row))))
        stmts.append("COMMIT")
        try:
            ExtSrcEnv._oracle_sql(
                cfg, 'DROP TABLE "%s" CASCADE CONSTRAINTS PURGE' % table,
                user=cfg.user, password=cfg.password)
        except RuntimeError as exc:
            if "ORA-00942" not in str(exc):
                raise
        ExtSrcEnv._oracle_sql(cfg, stmts, user=cfg.user, password=cfg.password)

    def cleanup(self, tables):
        cfg = next(ExtSrcEnv.oracle_version_configs())
        for table in tables:
            try:
                ExtSrcEnv.oracle_drop_table_cfg(cfg, table)
            except Exception as exc:  # noqa: BLE001 - best-effort teardown
                tdLog.info("oracle cleanup %s: %s" % (table, exc))


class _InfluxBackend(Backend):
    key = "influx"
    src_type = "influxdb"
    source = "vt_ttp_if"
    database = "vt_ttp_ifdb"
    # An InfluxDB integer field is a 64-bit integer, so the virtual column
    # has to be declared BIGINT to satisfy the type-compatibility check.
    v_decl = "bigint"

    def transport_rank(self, types):
        return "ns"

    def table_ref(self, table):
        return "%s.%s.%s" % (self.source, self.database, table)

    def create_measurement(self, table):
        """One int field per row; the time column is implicit."""
        lines = ["%s v=%di %d" % (table, row.v,
                                  row_epoch_ns(row, INFLUX_TYPES[0], "UTC"))
                 for row in ROWS]
        create_influx_measurement(self.database, lines)

    def cleanup(self, tables):
        try:
            ExtSrcEnv.influx_drop_db_cfg(ExtSrcEnv.influx_config(),
                                         self.database)
        except Exception as exc:  # noqa: BLE001 - best-effort teardown
            tdLog.info("influx cleanup: %s" % exc)

    def count_via_http(self, table):
        """Row count through InfluxDB's own HTTP SQL API.

        That API answers from the WAL, while FlightSQL (the protocol the
        connector uses) reads only persisted Parquet.  Comparing the two
        separates "fixture missing" from "server has the rows, connector
        cannot see them".
        """
        data = ExtSrcEnv.influx_query_sql_cfg(
            ExtSrcEnv.influx_config(), self.database,
            'SELECT count(*) FROM "%s"' % table)
        if not data:
            return 0
        first = data[0]
        for key in ("count(*)", "count"):
            if key in first:
                return int(first[key])
        return int(list(first.values())[0])

    def wait_visible(self, table, timeout=30):
        """InfluxDB 3 exposes WAL data to FlightSQL after a short delay."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                data = ExtSrcEnv.influx_query_sql_cfg(
                    ExtSrcEnv.influx_config(), self.database,
                    'SELECT count(*) FROM "%s"' % table)
                if data and int(data[0].get("count(*)", 0)) == len(ROWS):
                    return
            except Exception as exc:  # noqa: BLE001 - visibility probe
                tdLog.info("influx visibility probe failed: %s" % exc)
            time.sleep(1)
        raise RuntimeError("InfluxDB measurement %s did not become visible"
                           % table)


MYSQL = _MysqlBackend()
PG = _PgBackend()
SQLSERVER = _SqlServerBackend()
ORACLE = _OracleBackend()
INFLUX = _InfluxBackend()

# Order is also the pytest reporting order.
BACKENDS = [MYSQL, PG, INFLUX, SQLSERVER, ORACLE]


def backend_types(backend):
    return TYPES[backend.key]


def axis_types(backend):
    """Types that can be the remote primary key (the virtual-table time axis)."""
    return [t for t in backend_types(backend) if t.pk_ok]


def mixed_types(backend):
    """Every catalog type, for the non-key column matrix."""
    return list(backend_types(backend))


def find_type(backend, label):
    for ts_type in backend_types(backend):
        if ts_type.label == label:
            return ts_type
    raise KeyError("%s has no time type %r" % (backend.key, label))


# Primary key of the mixed-column table: a millisecond-class axis, legal in
# all three local precisions so one remote table serves ms / us / ns reads.
_MIXED_PK_LABEL = {
    "mysql": "datetime3",
    "pg": "timestamp3",
    "sqlserver": "datetime2_3",
    "oracle": "timestamp3",
}


def mixed_pk(backend):
    return find_type(backend, _MIXED_PK_LABEL[backend.key])
