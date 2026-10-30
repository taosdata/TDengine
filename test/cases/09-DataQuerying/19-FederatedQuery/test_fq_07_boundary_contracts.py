# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

"""Federated-query boundary contracts.

This case is deliberately data driven.  A case cannot be added without an
explicit expected result or error code, and every error path also proves that
the current client connection remains usable.
"""

from dataclasses import dataclass
from decimal import Decimal
import datetime as _datetime
import os
import re
import threading
import time
from typing import Iterable, List, Sequence, Tuple

import taos.field as _taos_field
from taos.cinterface import taos_fetch_fields_e
from taos.constants import FieldType

from new_test_framework.utils import tdLog, tdSql

from federated_query_common import (
    ExtSrcEnv,
    FederatedQueryCaseHelper,
    FederatedQueryTestMixin,
    TSDB_CODE_EXT_TYPE_NOT_MAPPABLE,
    _code,
)


TSDB_CODE_EXT_IDENTIFIER_TOO_LONG = _code("TSDB_CODE_EXT_IDENTIFIER_TOO_LONG")
TSDB_CODE_EXT_REQUIRED_COLUMNS_TOO_MANY = _code(
    "TSDB_CODE_EXT_REQUIRED_COLUMNS_TOO_MANY"
)
TSDB_CODE_EXT_REQUIRED_TAGS_TOO_MANY = _code(
    "TSDB_CODE_EXT_REQUIRED_TAGS_TOO_MANY"
)
TSDB_CODE_EXT_TYPE_CONVERT_FAILED = _code("TSDB_CODE_EXT_TYPE_CONVERT_FAILED")
TSDB_CODE_EXT_SYNTAX_UNSUPPORTED = _code("TSDB_CODE_EXT_SYNTAX_UNSUPPORTED")
TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG = _code(
    "TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG"
)


_MYSQL = "mysql"
_PG = "postgresql"
_INFLUX = "influxdb"
_SQLSERVER = "sqlserver"
_ORACLE = "oracle"
_ALL_SOURCES = (_MYSQL, _PG, _INFLUX, _SQLSERVER, _ORACLE)

_MYSQL_DB = "fq07_boundary_m"
_PG_DB = "fq07_boundary_p"
_INFLUX_DB = "fq07_boundary_i"
_SQLSERVER_DB = "fq07_boundary_s"
_LOCAL_DB = "fq07_boundary_local"

# A schema is a login user in Oracle, so the login account's own schema is the
# single namespace level every fq07 fixture table is created in (DS 5.3.2).
_ORACLE_SCHEMA = ExtSrcEnv.ORACLE_USER.upper()
_ORACLE_FIXTURE_TABLES = (
    "boundary_t", "schema_t", "unknown_t", "value_t", "quoted_t", "interp_t",
    "fetch_t", "structured_t",
)

_BOUNDARY_EPOCH_MS = (1704067200000, 1704067260000, 1704067320000)
_NCHAR_LIMIT_CHARS = 16379
_FETCH_BOUNDARY_ROWS = 8193
_NCHAR_VALUES = (
    "x" * (_NCHAR_LIMIT_CHARS - 1),
    "y" * _NCHAR_LIMIT_CHARS,
    "z" * (_NCHAR_LIMIT_CHARS + 1),
)

# SQL Server nvarchar holds characters but TDengine NCHAR budgets bytes
# (nvarchar carries UTF-16 on the wire), so the SQL Server fixtures stay inside
# the 16379-byte budget and the over-budget row exceeds it by characters.
_SQLSERVER_NCHAR_VALUES = (
    "x" * (_NCHAR_LIMIT_CHARS // 2 - 1),
    "y" * (_NCHAR_LIMIT_CHARS // 2),
    "z" * (_NCHAR_LIMIT_CHARS + 1),
)

_ORIG_TZ = None
_ORIG_DATETIME_EPOCH = None
_TZ_PINNED = False


def _refresh_process_tz():
    """Make a TZ change visible to libc (and the client's own conversions).

    POSIX exposes time.tzset(); the Windows CRT keeps the zone cached until
    _tzset() is called, and the client formats result timestamps through libc
    localtime(), so without the refresh a pinned TZ never reaches the output
    even though the TZ environment variable itself was updated.
    """
    if hasattr(time, "tzset"):
        time.tzset()
        return
    try:
        import ctypes

        ctypes.CDLL("ucrtbase")._tzset()
    except OSError:
        pass


def _pin_process_timezone_utc():
    global _ORIG_TZ, _ORIG_DATETIME_EPOCH, _TZ_PINNED
    if _TZ_PINNED:
        return

    _ORIG_TZ = os.environ.get("TZ")
    _ORIG_DATETIME_EPOCH = _taos_field._datetime_epoch
    os.environ["TZ"] = "UTC"
    _refresh_process_tz()
    _taos_field._datetime_epoch = _datetime.datetime.fromtimestamp(0)
    _TZ_PINNED = True


def _restore_process_timezone():
    global _TZ_PINNED
    if not _TZ_PINNED:
        return

    if _ORIG_TZ is None:
        os.environ.pop("TZ", None)
    else:
        os.environ["TZ"] = _ORIG_TZ
    _refresh_process_tz()
    _taos_field._datetime_epoch = _ORIG_DATETIME_EPOCH
    _TZ_PINNED = False


def _utc_datetime_literal(epoch_ms: int) -> str:
    value = _datetime.datetime.fromtimestamp(
        epoch_ms / 1000.0, tz=_datetime.timezone.utc
    )
    return value.strftime("%Y-%m-%d %H:%M:%S.") + f"{epoch_ms % 1000:03d}"


@dataclass(frozen=True)
class SourceSpec:
    key: str
    source: str
    database: str
    namespace: str
    timestamp_column: str

    @property
    def table(self) -> str:
        return f"{self.source}.boundary_t"

    @property
    def explicit_table(self) -> str:
        return f"{self.source}.{self.namespace}.boundary_t"


@dataclass(frozen=True)
class QueryCase:
    case_id: str
    coverage: Tuple[str, ...]
    sql_template: str
    expected_rows: Tuple[Tuple[object, ...], ...]
    expected_columns: Tuple[str, ...]
    sources: Tuple[str, ...] = _ALL_SOURCES


@dataclass(frozen=True)
class FieldSchema:
    """Client-visible TAOS_FIELD_E contract for an asserted result column."""

    name: str
    type_code: int
    precision: int
    scale: int
    bytes: int


_QUERY_CASES = (
    QueryCase(
        "FQ07-B-PATH-001",
        ("6.1", "6.3", "6.9-projection"),
        "select id as id, val as val, name as name from {table} order by id",
        ((1, 10, "alpha"), (2, 20, "beta"), (3, None, "")),
        ("id", "val", "name"),
    ),
    QueryCase(
        "FQ07-B-PATH-002",
        ("6.1", "6.3", "6.9-where-order-limit"),
        "select id as id, name as name from {table} where val >= 20 order by id limit 1",
        ((2, "beta"),),
        ("id", "name"),
    ),
    QueryCase(
        "FQ07-B-QUOTE-001",
        ("6.1", "6.3", "6.9-projection"),
        "select `select` as reserved_value from {source}.quoted_t order by `select`",
        ((10,), (20,)),
        ("reserved_value",),
    ),
    QueryCase(
        "FQ07-H-AGG-001",
        ("6.1", "6.8", "6.9-aggregate"),
        "select count(*) as total, count(id) as ids, sum(val) as val_sum, max(val) as val_max from {table}",
        ((3, 3, 30, 20),),
        ("total", "ids", "val_sum", "val_max"),
    ),
    QueryCase(
        "FQ07-H-AGG-COUNT-COL-001",
        ("6.1", "6.8", "6.9-aggregate-count-column"),
        "select count(*) as total, count(val) as val_count, sum(val) as val_sum, max(val) as val_max from {table}",
        ((3, 2, 30, 20),),
        ("total", "val_count", "val_sum", "val_max"),
    ),
    QueryCase(
        "FQ07-H-GROUP-001",
        ("6.1", "6.4", "6.9-group-having-distinct"),
        "select category as category, count(*) as total from {table} group by category having count(*) >= 1 order by category",
        (("a", 2), ("b", 1)),
        ("category", "total"),
    ),
    QueryCase(
        "FQ07-H-CASE-001",
        ("6.1", "6.8", "6.9-case-cast-arithmetic"),
        "select id as id, case when val is null then 'missing' when val >= 20 then 'high' else 'low' end as val_class from {table} order by id",
        ((1, "low"), (2, "high"), (3, "missing")),
        ("id", "val_class"),
    ),
    QueryCase(
        "FQ07-H-SUBQ-001",
        ("6.1", "6.4", "6.9-subquery"),
        "select id as id from {table} where val in (select val from {table} where val >= 20) order by id",
        ((2,),),
        ("id",),
    ),
    QueryCase(
        "FQ07-H-UNION-001",
        ("6.1", "6.9-union"),
        "select id as id from {table} where id = 1 union all select id as id from {table} where id = 2 order by id",
        ((1,), (2,)),
        ("id",),
    ),
    QueryCase(
        "FQ07-H-JOIN-001",
        ("6.1", "6.4", "6.9-join"),
        "select l.id as id, r.name as name from {table} l join {table} r on l.{timestamp} = r.{timestamp} where l.id = 2 order by l.id",
        ((2, "beta"),),
        ("id", "name"),
    ),
    QueryCase(
        "FQ07-H-DISTINCT-001",
        ("6.1", "6.4", "6.9-distinct"),
        "select distinct category as category from {table} order by category",
        (("a",), ("b",)),
        ("category",),
    ),
    QueryCase(
        "FQ07-H-HAVING-001",
        ("6.1", "6.4", "6.9-group-having"),
        "select category as category, count(*) as total, sum(val) as val_sum from {table} group by category having count(*) > 1 order by category",
        (("a", 2, 10),),
        ("category", "total", "val_sum"),
    ),
    QueryCase(
        "FQ07-H-LIMIT-001",
        ("6.1", "6.9-limit-offset"),
        "select id as id, name as name from {table} order by id limit 1 offset 1",
        ((2, "beta"),),
        ("id", "name"),
    ),
    QueryCase(
        "FQ07-H-SCALAR-001",
        ("6.1", "6.8", "6.9-scalar"),
        "select id as id, val + 1 as val_plus_one from {table} order by id",
        ((1, 11), (2, 21), (3, None)),
        ("id", "val_plus_one"),
    ),
    QueryCase(
        "FQ07-H-MULTI-AGG-001",
        ("6.1", "6.8", "6.9-multi-aggregate"),
        "select min(val) as val_min, max(val) as val_max, avg(val) as val_avg from {table}",
        ((10, 20, 15.0),),
        ("val_min", "val_max", "val_avg"),
    ),
    QueryCase(
        "FQ07-E-VALUE-001",
        ("6.1", "6.6", "6.8", "6.9-projection"),
        "select id as id, score as score, note as note, flag as flag from {table} order by id",
        ((1, 1.5, "", True), (2, 2.5, "中文", False), (3, None, "emoji🙂", None)),
        ("id", "score", "note", "flag"),
    ),
    QueryCase(
        "FQ07-H-EXT-LOCAL-JOIN-001",
        ("6.1", "6.8", "6.9-join-local"),
        "select e.id as id, d.weight as weight from {table} e join "
        f"{_LOCAL_DB}.dim d on e.{{timestamp}} = d.ts order by e.id",
        ((1, 100), (2, 200)),
        ("id", "weight"),
    ),
    QueryCase(
        "FQ07-H-LOCAL-EXT-JOIN-001",
        ("6.1", "6.8", "6.9-join-local-bidirectional"),
        f"select d.id as id, e.val as val from {_LOCAL_DB}.dim d join {{table}} e "
        "on d.ts = e.{timestamp} order by d.id",
        ((1, 10), (2, 20)),
        ("id", "val"),
    ),
    QueryCase(
        "FQ07-H-LEFT-JOIN-001",
        ("6.1", "6.4", "6.9-left-join"),
        "select l.id as id, r.val as joined_val from {table} l left join {table} r "
        "on l.{timestamp} = r.{timestamp} order by l.id",
        ((1, 10), (2, 20), (3, None)),
        ("id", "joined_val"),
    ),
    QueryCase(
        "FQ07-H-RIGHT-JOIN-001",
        ("6.1", "6.4", "6.9-right-join"),
        "select r.id as id, l.val as joined_val from {table} l right join {table} r "
        "on l.{timestamp} = r.{timestamp} order by r.id",
        ((1, 10), (2, 20), (3, None)),
        ("id", "joined_val"),
    ),
    QueryCase(
        "FQ07-H-FULL-JOIN-001",
        ("6.1", "6.4", "6.9-full-join"),
        "select l.id as left_id, r.id as right_id from {table} l full outer join {table} r "
        "on l.{timestamp} = r.{timestamp} order by l.id",
        ((1, 1), (2, 2), (3, 3)),
        ("left_id", "right_id"),
    ),
    QueryCase(
        "FQ07-H-SEMI-JOIN-001",
        ("6.1", "6.4", "6.9-semi-join"),
        "select l.id as id from {table} l left semi join {table} r "
        "on l.{timestamp} = r.{timestamp} order by l.id",
        ((1,), (2,), (3,)),
        ("id",),
    ),
    QueryCase(
        "FQ07-H-ANTI-JOIN-001",
        ("6.1", "6.4", "6.9-anti-join"),
        "select l.id as id from {table} l left anti join {table} r "
        "on l.{timestamp} = r.{timestamp} order by l.id",
        (),
        ("id",),
    ),
    QueryCase(
        "FQ07-H-ASOF-JOIN-001",
        ("6.1", "6.4", "6.9-asof-join"),
        "select l.id as id, r.val as joined_val from {table} l left asof join {table} r "
        "on l.{timestamp} >= r.{timestamp} order by l.id",
        ((1, 10), (2, 20), (3, None)),
        ("id", "joined_val"),
    ),
    QueryCase(
        "FQ07-H-SCALAR-SUBQUERY-001",
        ("6.1", "6.4", "6.9-scalar-subquery"),
        "select id as id, val as val, (select max(val) from {table}) as max_val "
        "from {table} order by id",
        ((1, 10, 20), (2, 20, 20), (3, None, 20)),
        ("id", "val", "max_val"),
    ),
    QueryCase(
        "FQ07-H-EXISTS-001",
        ("6.1", "6.4", "6.9-exists"),
        "select id as id from {table} where exists "
        "(select 1 from {table} where val >= 20) order by id",
        ((1,), (2,), (3,)),
        ("id",),
    ),
    QueryCase(
        "FQ07-H-ALL-ANY-SOME-001",
        ("6.1", "6.4", "6.9-all-any-some"),
        "select id as id from {table} where val > any "
        "(select val from {table} where val < 20) order by id",
        ((2,),),
        ("id",),
    ),
    QueryCase(
        "FQ07-H-NOT-IN-001",
        ("6.1", "6.4", "6.9-not-in"),
        "select id as id from {table} where id not in "
        "(select id from {table} where val >= 20) order by id",
        ((1,), (3,)),
        ("id",),
    ),
    QueryCase(
        "FQ07-H-NOT-EXISTS-001",
        ("6.1", "6.4", "6.9-not-exists"),
        "select id as id from {table} where not exists "
        "(select 1 from {table} where val > 999) order by id",
        ((1,), (2,), (3,)),
        ("id",),
    ),
    QueryCase(
        "FQ07-H-SOME-001",
        ("6.1", "6.4", "6.9-some"),
        "select id as id from {table} where val > some "
        "(select val from {table} where val < 20) order by id",
        ((2,),),
        ("id",),
    ),
    QueryCase(
        "FQ07-H-DERIVED-001",
        ("6.1", "6.4", "6.9-derived-subquery"),
        "select q.id as id, q.doubled as doubled from "
        "(select id, val * 2 as doubled from {table} where id < 3) q order by q.id",
        ((1, 20), (2, 40)),
        ("id", "doubled"),
    ),
    QueryCase(
        "FQ07-H-WINDOW-JOIN-001",
        ("6.1", "6.4", "6.9-window-join"),
        "select l.id as id, r.val as joined_val from {table} l left window join {table} r "
        "window_offset(0s, 0s) order by l.{timestamp}, r.{timestamp}",
        ((1, 10), (2, 20), (3, None)),
        ("id", "joined_val"),
    ),
    QueryCase(
        "FQ07-H-UNION-DISTINCT-001",
        ("6.1", "6.9-union-distinct"),
        "select category as category from {table} where id <= 2 union "
        "select category as category from {table} where id >= 2 order by category",
        (("a",), ("b",)),
        ("category",),
    ),
    QueryCase(
        "FQ07-H-INTERVAL-001",
        ("6.1", "6.8", "6.9-interval-window"),
        "select count(*) as total, sum(val) as val_sum from {table} "
        "interval(1m) order by _wstart",
        ((1, 10), (1, 20), (1, None)),
        ("total", "val_sum"),
    ),
    QueryCase(
        "FQ07-H-FILL-001",
        ("6.1", "6.8", "6.9-fill"),
        "select count(*) as total from {table} where {timestamp} >= '2024-01-01 00:00:00' "
        "and {timestamp} < '2024-01-01 00:05:00' interval(1m) fill(value, 0) order by _wstart",
        ((1,), (1,), (1,), (0,), (0,)),
        ("total",),
    ),
    QueryCase(
        "FQ07-H-INTERP-001",
        ("6.1", "6.8", "6.9-interp"),
        "select interp(val) as val_interp from {source}.interp_t "
        "range('2024-01-01 00:00:00', '2024-01-01 00:02:00') every(1m) fill(linear)",
        ((10,), (20,), (30,)),
        ("val_interp",),
    ),
    QueryCase(
        "FQ07-H-COUNT-WINDOW-001",
        ("6.1", "6.8", "6.9-count-window"),
        "select count(*) as total, sum(val) as val_sum from {table} "
        "count_window(2) order by _wstart",
        ((2, 30), (1, None)),
        ("total", "val_sum"),
    ),
    QueryCase(
        "FQ07-H-SESSION-001",
        ("6.1", "6.8", "6.9-session-window"),
        "select count(*) as total, sum(val) as val_sum from {table} "
        "session({timestamp}, 2m)",
        ((3, 30),),
        ("total", "val_sum"),
    ),
    QueryCase(
        "FQ07-H-STATE-001",
        ("6.1", "6.8", "6.9-state-window"),
        "select count(*) as total from {table} state_window(category)",
        ((1,), (1,), (1,)),
        ("total",),
    ),
    QueryCase(
        "FQ07-H-EVENT-001",
        ("6.1", "6.8", "6.9-event-window"),
        "select count(*) as total, sum(val) as val_sum from {table} "
        "event_window start with val >= 20 end with val is null",
        ((2, 20),),
        ("total", "val_sum"),
    ),
    QueryCase(
        "FQ07-H-SLIDING-001",
        ("6.1", "6.8", "6.9-interval-sliding"),
        "select count(*) as total, sum(val) as val_sum from {table} "
        "interval(2m) sliding(1m) order by _wstart",
        ((1, 10), (2, 30), (2, 20), (1, None)),
        ("total", "val_sum"),
    ),
    QueryCase(
        "FQ07-H-PARTITION-001",
        ("6.1", "6.4", "6.9-partition"),
        "select category as category, count(*) as total from {table} "
        "partition by category interval(1m) order by category, _wstart",
        (("a", 1), ("a", 1), ("b", 1)),
        ("category", "total"),
    ),
)

# Per-source expected rows for the two cases that assert a column whose Oracle
# value differs from the shared data set: Oracle stores '' as NULL, and
# NUMBER(1,0) maps to TINYINT (never BOOL), so the flag reads back as 1/0.
# Every other case asserts values Oracle represents identically.
_ORACLE_ROW_OVERRIDES = {
    "FQ07-B-PATH-001": ((1, 10, "alpha"), (2, 20, "beta"), (3, None, None)),
    "FQ07-E-VALUE-001": (
        (1, 1.5, None, 1),
        (2, 2.5, "中文", 0),
        (3, None, "emoji🙂", None),
    ),
}


class TestFq07BoundaryContracts(FederatedQueryTestMixin):
    """Boundary contract coverage for the five real external libraries."""

    updatecfgDict = {
        "federatedQueryEnable": 1,
        "queryPlannerTrace": 1,
        "clientCfg": {"federatedQueryEnable": 1, "queryPlannerTrace": 1},
    }
    _remote_prepared = False

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")
        self.helper = FederatedQueryCaseHelper(__file__)
        self.helper.require_external_source_feature()
        ExtSrcEnv.ensure_env()
        self.specs = {
            _MYSQL: SourceSpec(_MYSQL, "fq07_src_m", _MYSQL_DB, _MYSQL_DB, "ts"),
            _PG: SourceSpec(_PG, "fq07_src_p", _PG_DB, "public", "ts"),
            _INFLUX: SourceSpec(_INFLUX, "fq07_src_i", _INFLUX_DB, _INFLUX_DB, "time"),
            _SQLSERVER: SourceSpec(_SQLSERVER, "fq07_src_s", _SQLSERVER_DB, "dbo", "ts"),
            _ORACLE: SourceSpec(_ORACLE, "fq07_src_o", ExtSrcEnv.ORACLE_SERVICE,
                                _ORACLE_SCHEMA, "ts"),
        }

    def setup_method(self, method):
        if TestFq07BoundaryContracts._remote_prepared:
            return
        _pin_process_timezone_utc()
        tdSql.execute('ALTER LOCAL "timezone" "UTC"')
        self._prepare_remote_data()
        self._prepare_local_data()
        self._create_primary_sources()
        TestFq07BoundaryContracts._remote_prepared = True

    def teardown_class(self):
        tdLog.debug(f"teardown {__file__}")
        TestFq07BoundaryContracts._remote_prepared = False
        _restore_process_timezone()

    # --- assertion primitives ---

    @staticmethod
    def _rows() -> List[Tuple[object, ...]]:
        return [tuple(row) for row in (tdSql.queryResult or [])]

    def _assert_rows(self, sql: str, expected_rows: Sequence[Sequence[object]]):
        self.helper.assert_query_result(sql, [tuple(row) for row in expected_rows])

    def _assert_result_columns(self, sql: str, expected_columns: Sequence[str]):
        actual_columns = tuple(str(column[0]).lower() for column in (tdSql.cursor.description or ()))
        expected = tuple(column.lower() for column in expected_columns)
        assert actual_columns == expected, (
            f"result schema mismatch for {sql}: expected={expected!r}, actual={actual_columns!r}"
        )

    def _assert_field_schema(self, sql: str, expected_fields: Sequence[FieldSchema],
                             expected_rows: Sequence[Sequence[object]]):
        """Assert complete rows and every TAOS_FIELD_E member from one execution."""
        tdSql.query(sql)
        actual_rows = tuple(self._rows())
        expected = tuple(tuple(row) for row in expected_rows)
        assert actual_rows == expected, (
            f"field-metadata query result mismatch for {sql}: "
            f"expected={expected!r}, actual={actual_rows!r}"
        )
        fields = tuple(taos_fetch_fields_e(tdSql.cursor._result))
        actual = tuple(
            FieldSchema(field.name, field.type, field.precision, field.scale, field.bytes)
            for field in fields
        )
        assert actual == tuple(expected_fields), (
            f"TAOS_FIELD_E mismatch for {sql}: expected={expected_fields!r}, actual={actual!r}"
        )

    def _assert_error(self, sql: str, errno: int, safe_message: str = "",
                      forbidden: Iterable[str] = ()):
        assert errno is not None, f"missing expected error code for SQL: {sql}"
        tdSql.error(sql, expectedErrno=errno, expectErrInfo=safe_message or None,
                    fullMatched=False)
        err_text = str(getattr(tdSql, "error_info", ""))
        if safe_message:
            assert safe_message.lower() in err_text.lower(), (
                f"error message lacks {safe_message!r}: {err_text!r}"
            )
        for secret in forbidden:
            assert secret not in err_text, f"secret leaked in error text: {secret!r}"
        self._assert_rows("select 1", ((1,),))

    def _assert_source_exists(self, name: str):
        self._assert_rows(
            "select source_name from information_schema.ins_ext_sources "
            f"where source_name = '{name}'",
            ((name,),),
        )

    def _assert_source_absent(self, name: str):
        self._assert_rows(
            "select source_name from information_schema.ins_ext_sources "
            f"where source_name = '{name}'",
            (),
        )

    @staticmethod
    def _source_ref(name: str) -> str:
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) and name.lower() != "select":
            return name
        return "`" + name.replace("`", "``") + "`"

    def _drop_source(self, name: str):
        tdSql.query(
            "select source_name from information_schema.ins_ext_sources "
            f"where source_name = '{name}'"
        )
        if self._rows():
            tdSql.execute(f"drop external source {self._source_ref(name)}")
        self._assert_source_absent(name)

    def _drop_oracle_fixtures(self):
        """Drop the fq07 tables from the Oracle login schema.

        Oracle has no per-test database to drop: a schema is a login user, so
        the fixtures are the only objects this suite creates there.
        """
        oracle_cfg = self._oracle_cfg()
        for table in _ORACLE_FIXTURE_TABLES:
            ExtSrcEnv.oracle_drop_table_cfg(oracle_cfg, table.upper())

    def _cleanup_runtime_data(self):
        for spec in self.specs.values():
            self._drop_source(spec.source)
        for cleanup in (
            lambda: ExtSrcEnv.mysql_drop_db_cfg(self._mysql_cfg(), _MYSQL_DB),
            lambda: ExtSrcEnv.pg_drop_db_cfg(self._pg_cfg(), _PG_DB),
            lambda: ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), _INFLUX_DB),
            lambda: ExtSrcEnv.sqlserver_drop_db(_SQLSERVER_DB),
            self._drop_oracle_fixtures,
        ):
            try:
                cleanup()
            except Exception as exc:
                tdLog.info(f"FQ07 cleanup warning: {exc}")
        tdSql.execute(f"drop database if exists {_LOCAL_DB}")
        self._assert_rows(
            "select name from information_schema.ins_databases "
            f"where name = '{_LOCAL_DB}'",
            (),
        )

    def _assert_source_config_value(self, name: str, field: str, expected: str):
        assert field in {"database", "schema"}, f"unsupported source config field: {field}"
        self._assert_rows(
            f"select `{field}` from information_schema.ins_ext_sources "
            f"where source_name = '{name}'",
            ((expected,),),
        )

    def _assert_describe_source_summary(self, name: str, source_type: str):
        self._assert_rows(
            f"describe external source {self._source_ref(name)}",
            ((name, source_type),),
        )

    # --- source and remote-data setup ---

    def _source_sql(self, spec: SourceSpec, name: str = "", database: str = "",
                    schema: str = "", options: str = "") -> str:
        name = name or spec.source
        database = spec.database if database == "" else database
        if spec.key == _MYSQL:
            cfg = self._mysql_cfg()
            sql = (f"create external source {self._source_ref(name)} type='mysql' host='{cfg.host}' "
                   f"port={cfg.port} user='{cfg.user}' password='{cfg.password}'")
        elif spec.key == _PG:
            cfg = self._pg_cfg()
            sql = (f"create external source {self._source_ref(name)} type='postgresql' host='{cfg.host}' "
                   f"port={cfg.port} user='{cfg.user}' password='{cfg.password}'")
        elif spec.key == _SQLSERVER:
            cfg = self._sqlserver_cfg()
            sql = (f"create external source {self._source_ref(name)} type='sqlserver' host='{cfg.host}' "
                   f"port={cfg.port} user='{cfg.user}' password='{cfg.password}'")
        elif spec.key == _ORACLE:
            cfg = self._oracle_cfg()
            sql = (f"create external source {self._source_ref(name)} type='oracle' host='{cfg.host}' "
                   f"port={cfg.port} user='{cfg.user}' password='{cfg.password}'")
        else:
            cfg = self._influx_cfg()
            sql = (f"create external source {self._source_ref(name)} type='influxdb' host='{cfg.host}' "
                   f"port={cfg.port} user='fq07' password=''" )
            options = options or self._fmt_influx_options(cfg.token)
        if database:
            sql += f" database='{database}'"
        if spec.key == _PG and schema:
            sql += f" schema='{schema}'"
        if spec.key == _SQLSERVER:
            # The shared helper passes PG's default; SQL Server's default schema is dbo.
            sqlserver_schema = "dbo" if schema in ("", None, "public") else schema
            sql += f" schema='{sqlserver_schema}'"
        if spec.key == _ORACLE and schema:
            # DATABASE carries the service name, SCHEMA the single namespace
            # level.  Oracle negotiates its own encryption, so no TLS keys are
            # ever emitted (DS 5.3.2 / 6.1.1).
            sql += f" schema='{schema}'"
        if options:
            sql += f" options({options})"
        return sql

    def _create_primary_sources(self):
        for spec in self.specs.values():
            self._drop_source(spec.source)
            if spec.key == _PG:
                tdSql.execute(self._source_sql(spec, schema="public"))
            elif spec.key == _SQLSERVER:
                tdSql.execute(self._source_sql(spec, schema="dbo"))
            elif spec.key == _ORACLE:
                tdSql.execute(self._source_sql(spec, schema=_ORACLE_SCHEMA))
            else:
                tdSql.execute(self._source_sql(spec))
            self._assert_source_exists(spec.source)
            self._assert_describe_source_summary(spec.source, spec.key)
            self._assert_source_config_value(spec.source, "database", spec.database)
            if spec.key == _PG:
                self._assert_source_config_value(spec.source, "schema", "public")
            if spec.key == _SQLSERVER:
                self._assert_source_config_value(spec.source, "schema", "dbo")
            if spec.key == _ORACLE:
                self._assert_source_config_value(spec.source, "schema", _ORACLE_SCHEMA)

    def _prepare_remote_data(self):
        first_dt, second_dt, third_dt = (
            _utc_datetime_literal(epoch_ms) for epoch_ms in _BOUNDARY_EPOCH_MS
        )
        mysql_cfg = self._mysql_cfg()
        ExtSrcEnv.mysql_create_db_cfg(mysql_cfg, _MYSQL_DB)
        ExtSrcEnv.mysql_exec_cfg(mysql_cfg, _MYSQL_DB, [
            "drop table if exists boundary_t",
            "drop table if exists schema_t",
            "drop table if exists unknown_t",
            "drop table if exists value_t",
            "drop table if exists quoted_t",
            "drop table if exists interp_t",
            "drop table if exists fetch_t",
            "drop table if exists structured_t",
            "create table boundary_t ("
            "ts datetime(3) primary key, id int, val int, score double, "
            "name varchar(32), category varchar(8), note varchar(32), "
            "dec_ok decimal(18,4), flag tinyint(1)) character set utf8mb4",
            "insert into boundary_t values "
            f"('{first_dt}',1,10,1.5,'alpha','a','',123.4567,1),"
            f"('{second_dt}',2,20,2.5,'beta','b','中文',-1.0000,0),"
            f"('{third_dt}',3,null,null,'','a','emoji🙂',null,null)",
            "create table schema_t (ts datetime(3) primary key, c_char char(4), "
            "c_varchar varchar(8), c_bin varbinary(8), c_blob blob, c_json json, "
            "c_dec decimal(18,4), c_geom point)",
            "insert into schema_t values ('2024-01-01 00:00:00.000','ab','text',"
            "x'00ff',x'aabb','{\"k\":1}',123.4567,st_pointfromtext('POINT(1 2)'))",
            "create table unknown_t (ts datetime(3) primary key, id int, shape multilinestring)",
            "insert into unknown_t values ('2024-01-01 00:00:00.000',1,"
            "st_geomfromtext('MULTILINESTRING((0 0,1 1),(2 2,3 3))'))",
            "create table value_t (id int primary key, note mediumtext) character set utf8mb4",
            "insert into value_t values "
            f"(1,'{_NCHAR_VALUES[0]}'),(2,'{_NCHAR_VALUES[1]}'),(3,'{_NCHAR_VALUES[2]}')",
            "create table quoted_t (ts datetime(3) primary key, `select` int)",
            f"insert into quoted_t values ('{first_dt}',10),('{second_dt}',20)",
            "create table interp_t (ts datetime(3) primary key, val int)",
            "insert into interp_t values ('2024-01-01 00:00:00.000',10),"
            "('2024-01-01 00:02:00.000',30)",
            "create table fetch_t (ts datetime(6) primary key, id int, val int)",
            "insert into fetch_t (ts, id, val) "
            "select timestampadd(second, n, '2024-02-01 00:00:00'), n + 1, (n + 1) * 3 "
            "from (select a.n + 10 * b.n + 100 * c.n + 1000 * d.n as n "
            "from (select 0 as n union all select 1 union all select 2 union all select 3 union all select 4 "
            "union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) a "
            "cross join (select 0 as n union all select 1 union all select 2 union all select 3 union all select 4 "
            "union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) b "
            "cross join (select 0 as n union all select 1 union all select 2 union all select 3 union all select 4 "
            "union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) c "
            "cross join (select 0 as n union all select 1 union all select 2 union all select 3 union all select 4 "
            "union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) d) seq "
            f"where n < {_FETCH_BOUNDARY_ROWS}",
            "create table structured_t (id int primary key, doc json)",
            "insert into structured_t values "
            "(1, json_object('a', json_array(1, 2), 'flag', true, 'name', 'v'))",
        ])
        assert ExtSrcEnv.mysql_query_cfg(mysql_cfg, _MYSQL_DB,
                                         "select count(*) from boundary_t") == 3
        assert ExtSrcEnv.mysql_query_cfg(
            mysql_cfg, _MYSQL_DB, "select count(*) from fetch_t"
        ) == _FETCH_BOUNDARY_ROWS

        pg_cfg = self._pg_cfg()
        ExtSrcEnv.pg_create_db_cfg(pg_cfg, _PG_DB)
        ExtSrcEnv.pg_exec_cfg(pg_cfg, _PG_DB, [
            "drop table if exists boundary_t",
            "drop table if exists schema_t",
            "drop table if exists unknown_t",
            "drop table if exists value_t",
            "drop table if exists quoted_t",
            "drop table if exists interp_t",
            "drop table if exists fetch_t",
            "drop table if exists structured_t",
            "create table boundary_t ("
            "ts timestamp primary key, id int, val int, score double precision, "
            "name varchar(32), category varchar(8), note text, "
            "dec_ok numeric(18,4), flag boolean)",
            "insert into boundary_t values "
            f"('{first_dt}',1,10,1.5,'alpha','a','',123.4567,true),"
            f"('{second_dt}',2,20,2.5,'beta','b','中文',-1.0000,false),"
            f"('{third_dt}',3,null,null,'','a','emoji🙂',null,null)",
            "create table schema_t (ts timestamp primary key, c_char char(4), "
            "c_varchar varchar(8), c_bin bytea, c_json jsonb, c_xml xml, "
            "c_array int[], c_dec numeric(18,4))",
            "insert into schema_t values ('2024-01-01 00:00:00.000','ab','text',"
            "decode('00ff','hex'),'{\"k\":1}','<a>1</a>',array[1,2],123.4567)",
            "create table unknown_t (ts timestamp primary key, id int, shape circle)",
            "insert into unknown_t values ('2024-01-01 00:00:00.000',1,'<(1,2),3>')",
            "create table value_t (id int primary key, note varchar(20000))",
            "insert into value_t values "
            f"(1,'{_NCHAR_VALUES[0]}'),(2,'{_NCHAR_VALUES[1]}'),(3,'{_NCHAR_VALUES[2]}')",
            "create table quoted_t (ts timestamp primary key, \"select\" int)",
            f"insert into quoted_t values ('{first_dt}',10),('{second_dt}',20)",
            "create table interp_t (ts timestamp primary key, val int)",
            "insert into interp_t values ('2024-01-01 00:00:00.000',10),"
            "('2024-01-01 00:02:00.000',30)",
            "create table fetch_t (ts timestamp primary key, id int, val int)",
            "insert into fetch_t (ts, id, val) "
            "select timestamp '2024-02-01 00:00:00' + n * interval '1 second', n, n * 3 "
            f"from generate_series(1, {_FETCH_BOUNDARY_ROWS}) as n",
            "create table structured_t (id int primary key, doc jsonb)",
            "insert into structured_t values "
            "(1, '{\"a\":[1,2],\"flag\":true,\"name\":\"v\"}'::jsonb)",
        ])
        assert ExtSrcEnv.pg_query_cfg(pg_cfg, _PG_DB,
                                      "select count(*) from boundary_t") == [(3,)]
        assert ExtSrcEnv.pg_query_cfg(
            pg_cfg, _PG_DB, "select count(*) from fetch_t"
        ) == [(_FETCH_BOUNDARY_ROWS,)]

        # -- SQL Server ------------------------------------------------------
        sqlserver_cfg = self._sqlserver_cfg()
        ExtSrcEnv.sqlserver_create_db(_SQLSERVER_DB)
        ExtSrcEnv.sqlserver_exec(_SQLSERVER_DB, [
            "IF OBJECT_ID('dbo.boundary_t') IS NOT NULL DROP TABLE dbo.boundary_t",
            "IF OBJECT_ID('dbo.schema_t') IS NOT NULL DROP TABLE dbo.schema_t",
            "IF OBJECT_ID('dbo.unknown_t') IS NOT NULL DROP TABLE dbo.unknown_t",
            "IF OBJECT_ID('dbo.value_t') IS NOT NULL DROP TABLE dbo.value_t",
            "IF OBJECT_ID('dbo.quoted_t') IS NOT NULL DROP TABLE dbo.quoted_t",
            "IF OBJECT_ID('dbo.interp_t') IS NOT NULL DROP TABLE dbo.interp_t",
            "IF OBJECT_ID('dbo.fetch_t') IS NOT NULL DROP TABLE dbo.fetch_t",
            "IF OBJECT_ID('dbo.structured_t') IS NOT NULL DROP TABLE dbo.structured_t",
            "CREATE TABLE dbo.boundary_t ("
            " ts datetime2(3) NOT NULL PRIMARY KEY, id int, val int, score float, "
            " name nvarchar(32), category nvarchar(8), note nvarchar(32), "
            " dec_ok decimal(18,4), flag bit)",
            "INSERT INTO dbo.boundary_t VALUES "
            f"('{first_dt}',1,10,1.5,N'alpha',N'a',N'',123.4567,1),"
            f"('{second_dt}',2,20,2.5,N'beta',N'b',N'中文',-1.0000,0),"
            f"('{third_dt}',3,NULL,NULL,N'',N'a',N'emoji🙂',NULL,NULL)",
            "CREATE TABLE dbo.schema_t ("
            " ts datetime2(3) NOT NULL PRIMARY KEY, c_char char(4), "
            " c_varchar varchar(8), c_bin varbinary(8), c_json nvarchar(max), "
            " c_dec decimal(18,4), c_geom geometry)",
            "INSERT INTO dbo.schema_t VALUES ('2024-01-01T00:00:00.000','ab','text',"
            "0x00ff,N'{\"k\":1}',123.4567,geometry::STGeomFromText('POINT(1 2)',4326))",
            "CREATE TABLE dbo.unknown_t (ts datetime2(3) NOT NULL PRIMARY KEY, id int, shape hierarchyid)",
            "INSERT INTO dbo.unknown_t VALUES ('2024-01-01T00:00:00.000',1,hierarchyid::Parse('/1/'))",
            # The over-budget value lives in its own table: SQL Server is
            # zero-pushdown, so a scan converts whole blocks before the local
            # filter runs - keeping it in value_t would fail the id < 3 query
            # too (see the boundary check below).
            "CREATE TABLE dbo.value_t (id int NOT NULL PRIMARY KEY, note nvarchar(max))",
            "INSERT INTO dbo.value_t VALUES "
            f"(1,N'{_SQLSERVER_NCHAR_VALUES[0]}'),(2,N'{_SQLSERVER_NCHAR_VALUES[1]}')",
            "CREATE TABLE dbo.value_big_t (id int NOT NULL PRIMARY KEY, note nvarchar(max))",
            f"INSERT INTO dbo.value_big_t VALUES (3,N'{_SQLSERVER_NCHAR_VALUES[2]}')",
            "CREATE TABLE dbo.quoted_t (ts datetime2(3) NOT NULL PRIMARY KEY, [select] int)",
            f"INSERT INTO dbo.quoted_t VALUES ('{first_dt}',10),('{second_dt}',20)",
            "CREATE TABLE dbo.interp_t (ts datetime2(3) NOT NULL PRIMARY KEY, val int)",
            "INSERT INTO dbo.interp_t VALUES ('2024-01-01T00:00:00.000',10),"
            "('2024-01-01T00:02:00.000',30)",
            "CREATE TABLE dbo.fetch_t (ts datetime2(3) NOT NULL PRIMARY KEY, id int, val int)",
            ";WITH seq AS (SELECT TOP ("
            f"{_FETCH_BOUNDARY_ROWS}"
            ") ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS k "
            "FROM sys.all_objects a CROSS JOIN sys.all_objects b) "
            "INSERT INTO dbo.fetch_t (ts, id, val) "
            "SELECT DATEADD(second, k, CAST('2024-02-01T00:00:00' AS datetime2(3))), k, k * 3 "
            "FROM seq",
            "CREATE TABLE dbo.structured_t (id int NOT NULL PRIMARY KEY, doc nvarchar(max))",
            "INSERT INTO dbo.structured_t VALUES "
            "(1, N'{\"a\": [1, 2], \"flag\": true, \"name\": \"v\"}')",
        ])
        assert ExtSrcEnv.sqlserver_query(
            _SQLSERVER_DB, "SELECT COUNT(*) FROM dbo.boundary_t") == "3"
        assert ExtSrcEnv.sqlserver_query(
            _SQLSERVER_DB, "SELECT COUNT(*) FROM dbo.fetch_t"
        ) == str(_FETCH_BOUNDARY_ROWS)

        # -- Oracle ----------------------------------------------------------
        # A schema is a login user (DS 5.3.2): the fixtures live in the login
        # schema and the source's SCHEMA default names it.  Oracle stores '' as
        # NULL, so the shared data set's empty strings are inserted as '' and
        # read back as NULL - the row overrides state that explicitly.
        oracle_cfg = self._oracle_cfg()
        for table in _ORACLE_FIXTURE_TABLES:
            ExtSrcEnv.oracle_drop_table_cfg(oracle_cfg, table.upper())
        ExtSrcEnv.oracle_exec_cfg(oracle_cfg, [
            (
                "CREATE TABLE boundary_t ("
                "ts TIMESTAMP(3), id NUMBER(10,0), val NUMBER(10,0), "
                "score BINARY_DOUBLE, name VARCHAR2(32), category VARCHAR2(8), "
                "note VARCHAR2(32), dec_ok NUMBER(18,4), flag NUMBER(1,0))"
            ),
            # 11g has no multi-row VALUES clause: INSERT ALL is the portable
            # form for the three boundary rows.
            (
                "INSERT ALL "
                "INTO boundary_t (ts, id, val, score, name, category, note, "
                "dec_ok, flag) VALUES "
                f"(TO_TIMESTAMP('{first_dt}', 'YYYY-MM-DD HH24:MI:SS.FF3'),"
                " 1, 10, 1.5, 'alpha', 'a', '', 123.4567, 1) "
                "INTO boundary_t (ts, id, val, score, name, category, note, "
                "dec_ok, flag) VALUES "
                f"(TO_TIMESTAMP('{second_dt}', 'YYYY-MM-DD HH24:MI:SS.FF3'),"
                " 2, 20, 2.5, 'beta', 'b', '中文', -1.0000, 0) "
                "INTO boundary_t (ts, id, val, score, name, category, note, "
                "dec_ok, flag) VALUES "
                f"(TO_TIMESTAMP('{third_dt}', 'YYYY-MM-DD HH24:MI:SS.FF3'),"
                " 3, NULL, NULL, '', 'a', 'emoji🙂', NULL, NULL) "
                "SELECT 1 FROM dual"
            ),
            # SDO_GEOMETRY is deliberately absent: object types are not
            # mappable (DS 5.3.2.1), so one here would make the whole table
            # undescribable.  The unmappable family is exercised by unknown_t.
            (
                "CREATE TABLE schema_t ("
                "ts TIMESTAMP(3), c_char CHAR(4), c_varchar VARCHAR2(8), "
                "c_bin RAW(8), c_clob CLOB, c_dec NUMBER(18,4))"
            ),
            (
                "INSERT INTO schema_t VALUES ("
                "TIMESTAMP '2024-01-01 00:00:00.000', 'ab', 'text', "
                "HEXTORAW('00FF'), '{\"k\":1}', 123.4567)"
            ),
            (
                "CREATE TABLE unknown_t ("
                "ts TIMESTAMP(3), id NUMBER(10,0), shape INTERVAL DAY TO SECOND)"
            ),
            (
                "INSERT INTO unknown_t VALUES ("
                "TIMESTAMP '2024-01-01 00:00:00.000', 1, "
                "INTERVAL '1 02:03:04' DAY TO SECOND)"
            ),
            "CREATE TABLE value_t (id NUMBER(10,0), note CLOB)",
            "CREATE TABLE quoted_t (ts TIMESTAMP(3), \"SELECT\" NUMBER(10,0))",
            (
                "INSERT ALL "
                "INTO quoted_t VALUES "
                f"(TO_TIMESTAMP('{first_dt}', 'YYYY-MM-DD HH24:MI:SS.FF3'), 10) "
                "INTO quoted_t VALUES "
                f"(TO_TIMESTAMP('{second_dt}', 'YYYY-MM-DD HH24:MI:SS.FF3'), 20) "
                "SELECT 1 FROM dual"
            ),
            "CREATE TABLE interp_t (ts TIMESTAMP(3), val NUMBER(10,0))",
            (
                "INSERT ALL "
                "INTO interp_t VALUES (TIMESTAMP '2024-01-01 00:00:00.000', 10) "
                "INTO interp_t VALUES (TIMESTAMP '2024-01-01 00:02:00.000', 30) "
                "SELECT 1 FROM dual"
            ),
            "CREATE TABLE fetch_t (ts TIMESTAMP(3), id NUMBER(10,0), val NUMBER(10,0))",
            (
                "INSERT INTO fetch_t (ts, id, val) "
                "SELECT TIMESTAMP '2024-02-01 00:00:00' + NUMTODSINTERVAL(level - 1, 'SECOND'), "
                f"level, level * 3 FROM dual CONNECT BY level <= {_FETCH_BOUNDARY_ROWS}"
            ),
            "CREATE TABLE structured_t (id NUMBER(10,0), doc VARCHAR2(4000))",
            (
                "INSERT INTO structured_t VALUES "
                "(1, '{\"a\": [1, 2], \"flag\": true, \"name\": \"v\"}')"
            ),
            "COMMIT",
        ])
        # A VARCHAR2 literal is capped at 4000 bytes (ORA-01704 beyond that),
        # so the NCHAR boundary values are built as CLOBs: RPAD over a CLOB
        # returns a CLOB and is not subject to the VARCHAR2 limit.
        ExtSrcEnv.oracle_exec_cfg(oracle_cfg, [
            "INSERT INTO value_t (id, note) SELECT 1, "
            f"RPAD(TO_CLOB('x'), {_NCHAR_LIMIT_CHARS - 1}, TO_CLOB('x')) FROM dual",
            "INSERT INTO value_t (id, note) SELECT 2, "
            f"RPAD(TO_CLOB('y'), {_NCHAR_LIMIT_CHARS}, TO_CLOB('y')) FROM dual",
            "INSERT INTO value_t (id, note) SELECT 3, "
            f"RPAD(TO_CLOB('z'), {_NCHAR_LIMIT_CHARS + 1}, TO_CLOB('z')) FROM dual",
        ])
        assert ExtSrcEnv.oracle_query_cfg(
            oracle_cfg, "SELECT COUNT(*) FROM boundary_t") == "3"
        assert ExtSrcEnv.oracle_query_cfg(
            oracle_cfg, "SELECT COUNT(*) FROM fetch_t") == str(_FETCH_BOUNDARY_ROWS)
        assert ExtSrcEnv.oracle_query_cfg(
            oracle_cfg, "SELECT MAX(LENGTH(note)) FROM value_t"
        ) == str(_NCHAR_LIMIT_CHARS + 1)
        assert ExtSrcEnv.oracle_query_cfg(
            oracle_cfg, "SELECT COUNT(*) FROM value_t WHERE LENGTH(note) IN "
            f"({_NCHAR_LIMIT_CHARS - 1}, {_NCHAR_LIMIT_CHARS})"
        ) == "2"

        influx_cfg = self._influx_cfg()
        ExtSrcEnv.influx_create_db_cfg(influx_cfg, _INFLUX_DB)
        ExtSrcEnv.influx_write_cfg(influx_cfg, _INFLUX_DB, [
            "boundary_t,category=a id=1i,val=10i,score=1.5,name=\"alpha\",note=\"\",dec_ok=123.4567,flag=true 1704067200000000000",
            "boundary_t,category=b id=2i,val=20i,score=2.5,name=\"beta\",note=\"中文\",dec_ok=-1.0,flag=false 1704067260000000000",
            "boundary_t,category=a id=3i,name=\"\",note=\"emoji🙂\" 1704067320000000000",
            "schema_t c_int=1i,c_float=1.5,c_str=\"text\",c_bool=true 1704067200000000000",
            f"value_t id=1i,note=\"{_NCHAR_VALUES[0]}\" 1704067200000000000",
            f"value_t id=2i,note=\"{_NCHAR_VALUES[1]}\" 1704067260000000000",
            "quoted_t select=10i 1704067200000000000",
            "quoted_t select=20i 1704067260000000000",
            "interp_t val=10i 1704067200000000000",
            "interp_t val=30i 1704067320000000000",
            "http_sparse_t id=1i,val=10i,name=\"alpha\" 1704067200000000000",
            "http_sparse_t id=2i,val=20i,name=\"beta\" 1704067260000000000",
            "http_sparse_t id=3i,name=\"missing\" 1704067320000000000",
            "http_sparse_t id=4i,val=0i,name=\"zero\" 1704067380000000000",
            "structured_t id=1i,doc=\"{\\\"a\\\":[1,2],\\\"flag\\\":true,\\\"name\\\":\\\"v\\\"}\" 1704067200000000000",
            *(
                "fetch_t id=%di,val=%di %d" % (
                    index, index * 3, 1706745600000000000 + index * 1000000000
                )
                for index in range(1, _FETCH_BOUNDARY_ROWS + 1)
            ),
        ])
        influx_count = ExtSrcEnv.influx_query_sql_cfg(
            influx_cfg, _INFLUX_DB, "select count(*) from boundary_t"
        )
        assert influx_count is not None, "InfluxDB boundary dataset query returned no result"
        assert ExtSrcEnv.influx_query_sql_cfg(
            influx_cfg, _INFLUX_DB, "select count(*) as total from fetch_t"
        ) == [{"total": _FETCH_BOUNDARY_ROWS}]

    def _prepare_local_data(self):
        tdSql.execute(f"drop database if exists {_LOCAL_DB}")
        tdSql.execute(f"create database {_LOCAL_DB} precision 'ns'")
        tdSql.execute(f"use {_LOCAL_DB}")
        tdSql.execute("create table dim (ts timestamp, id int, weight int)")
        tdSql.execute(
            "insert into dim values "
            f"({_BOUNDARY_EPOCH_MS[0]}000000, 1, 100) "
            f"({_BOUNDARY_EPOCH_MS[1]}000000, 2, 200)"
        )
        self._assert_rows(
            "select id, weight from dim order by id",
            ((1, 100), (2, 200)),
        )

    # --- data-driven query matrix ---

    def _run_query_case(self, case: QueryCase):
        assert set(case.sources) == set(_ALL_SOURCES), (
            f"{case.case_id} must run through every real external library: "
            f"{', '.join(_ALL_SOURCES)}"
        )
        assert case.expected_rows is not None, f"{case.case_id} has no expected rows"
        assert case.expected_columns, f"{case.case_id} has no result-schema assertion"
        for source_key in case.sources:
            spec = self.specs[source_key]
            sql = case.sql_template.format(
                table=spec.table, source=spec.source, timestamp=spec.timestamp_column
            )
            expected_rows = case.expected_rows
            if source_key == _ORACLE:
                expected_rows = _ORACLE_ROW_OVERRIDES.get(case.case_id, expected_rows)
            self._assert_rows(sql, expected_rows)
            self._assert_result_columns(sql, case.expected_columns)

    def do_query_result_matrix(self):
        for case in _QUERY_CASES:
            self._run_query_case(case)
        print("FQ07 query result matrix ......................... [ passed ]")

    def do_influx_http_parity(self):
        source = "fq07_src_i_http"
        flight = self.specs[_INFLUX]
        cfg = self._influx_cfg()
        sql = "select id as id, val as val, name as name from {}.http_sparse_t order by id"
        expected = ((1, 10, "alpha"), (2, 20, "beta"),
                    (3, None, "missing"), (4, 0, "zero"))
        fields = (
            FieldSchema("id", FieldType.C_BIGINT, 0, 0, 8),
            FieldSchema("val", FieldType.C_BIGINT, 0, 0, 8),
            FieldSchema("name", FieldType.C_NCHAR, 0, 0, 16379),
        )
        aggregate = ("select count(*) as total, count(val) as val_count, sum(val) as val_sum, "
                     "max(val) as val_max from {}.http_sparse_t")
        self._drop_source(source)
        try:
            tdSql.execute(self._source_sql(
                flight, name=source,
                options=self._fmt_influx_options(cfg.token, protocol="http")))
            for source_name in (source, flight.source):
                self._assert_rows(sql.format(source_name), expected)
                self._assert_result_columns(sql.format(source_name), ("id", "val", "name"))
                self._assert_field_schema(sql.format(source_name), fields, expected)
                self._assert_rows(aggregate.format(source_name), ((4, 3, 30, 20),))
                self._assert_result_columns(
                    aggregate.format(source_name), ("total", "val_count", "val_sum", "val_max")
                )
        finally:
            self._drop_source(source)
        print("FQ07 Influx HTTP/Flight parity .................. [ passed ]")

    def do_native_fetch_batch_matrix(self):
        """Read more than two native provider batches with an exact row oracle."""
        flight = self.specs[_INFLUX]
        http_source = "fq07_src_i_http_batch"
        cfg = self._influx_cfg()
        expected = tuple((index, index * 3) for index in range(1, _FETCH_BOUNDARY_ROWS + 1))
        self._drop_source(http_source)
        try:
            tdSql.execute(self._source_sql(
                flight, name=http_source,
                options=self._fmt_influx_options(cfg.token, protocol="http")))
            source_specs = (
                (self.specs[_MYSQL].source, self.specs[_MYSQL].timestamp_column),
                (self.specs[_PG].source, self.specs[_PG].timestamp_column),
                (self.specs[_SQLSERVER].source, self.specs[_SQLSERVER].timestamp_column),
                (self.specs[_ORACLE].source, self.specs[_ORACLE].timestamp_column),
                (flight.source, flight.timestamp_column),
                (http_source, flight.timestamp_column),
            )
            for source, timestamp_column in source_specs:
                sql = (
                    "select e.id as id, e.val as val "
                    f"from {source}.fetch_t e left join {_LOCAL_DB}.dim d "
                    f"on e.{timestamp_column} = d.ts order by e.id"
                )
                self._assert_rows(sql, expected)
                self._assert_result_columns(sql, ("id", "val"))
                self._assert_remote_sql(sql, ("select",), ("federated", "join"))
                self._assert_rows("select 1", ((1,),))
        finally:
            self._drop_source(http_source)
        print("FQ07 native fetch batch matrix .................. [ passed ]")

    def do_native_structured_and_decimal_matrix(self):
        structured_expected = {
            _MYSQL: ((1, '{"a": [1, 2], "flag": true, "name": "v"}'),),
            _PG: ((1, '{"a": [1, 2], "flag": true, "name": "v"}'),),
            _SQLSERVER: ((1, '{"a": [1, 2], "flag": true, "name": "v"}'),),
            _ORACLE: ((1, '{"a": [1, 2], "flag": true, "name": "v"}'),),
            _INFLUX: ((1, '{"a":[1,2],"flag":true,"name":"v"}'),),
        }
        decimal_expected = {
            _MYSQL: ((1, Decimal("123.4567")), (2, Decimal("-1.0000")), (3, None)),
            _PG: ((1, Decimal("123.4567")), (2, Decimal("-1.0000")), (3, None)),
            _SQLSERVER: ((1, Decimal("123.4567")), (2, Decimal("-1.0000")), (3, None)),
            _ORACLE: ((1, Decimal("123.4567")), (2, Decimal("-1.0000")), (3, None)),
            _INFLUX: ((1, 123.4567), (2, -1.0), (3, None)),
        }
        for spec in self.specs.values():
            structured_sql = (
                f"select id as id, doc as doc from {spec.source}.structured_t order by id"
            )
            self._assert_rows(structured_sql, structured_expected[spec.key])
            self._assert_result_columns(structured_sql, ("id", "doc"))
            self._assert_remote_sql(structured_sql, ("select", "structured_t"), ("federated",))

            decimal_sql = f"select id as id, dec_ok as dec_ok from {spec.table} order by id"
            self._assert_rows(decimal_sql, decimal_expected[spec.key])
            self._assert_result_columns(decimal_sql, ("id", "dec_ok"))
            self._assert_remote_sql(decimal_sql, ("select", "dec_ok"), ("federated",))
            self._assert_rows("select 1", ((1,),))
        print("FQ07 native structured and decimal matrix ....... [ passed ]")

    def do_path_and_explain_matrix(self):
        expected = ((1, "alpha"), (2, "beta"), (3, ""))
        oracle_expected = ((1, "alpha"), (2, "beta"), (3, None))
        for spec in self.specs.values():
            self._assert_rows(
                f"select id, name from {spec.explicit_table} order by id",
                oracle_expected if spec.key == _ORACLE else expected,
            )
            self._assert_explain_has_federated_scan(
                f"select id, name from {spec.table} where id >= 1 order by id"
            )
            self._assert_plan_has_federated_scan(
                f"select id, name from {spec.table} where id >= 1 order by id"
            )
            self._assert_explain_analyze_has_federated_scan(
                f"select id, name from {spec.table} where id >= 1 order by id"
            )
        print("FQ07 path and explain matrix ..................... [ passed ]")

    def do_use_context_matrix(self):
        expected_external = ((1, "alpha"), (2, "beta"), (3, ""))
        oracle_expected_external = ((1, "alpha"), (2, "beta"), (3, None))
        for spec in self.specs.values():
            expected = (oracle_expected_external if spec.key == _ORACLE
                        else expected_external)
            tdSql.execute(f"use {self._source_ref(spec.source)}")
            self._assert_rows(
                "select id, name from boundary_t order by id", expected
            )

            if spec.key == _SQLSERVER:
                # Two-segment USE switches the database for SQL Server; the schema
                # comes from the source default (DS 5.3.4).
                tdSql.execute(
                    f"use {self._source_ref(spec.source)}.{self._source_ref(spec.database)}")
            else:
                # For Oracle the second segment is the schema, exactly as in a
                # three-segment path (DS 5.3.4).
                tdSql.execute(
                    f"use {self._source_ref(spec.source)}.{self._source_ref(spec.namespace)}")
            self._assert_rows(
                "select id, name from boundary_t order by id", expected
            )

            tdSql.execute(f"use {_LOCAL_DB}")
            self._assert_rows("select id, weight from dim order by id", ((1, 100), (2, 200)))
        print("FQ07 USE context matrix .......................... [ passed ]")

    def _assert_explain_has_federated_scan(self, sql: str):
        tdSql.query(f"explain {sql}")
        rows = self._rows()
        assert rows, f"EXPLAIN returned no rows: {sql}"
        text = "\n".join(str(cell) for row in rows for cell in row)
        assert "federated" in text.lower(), f"plan lacks federated scan: {text}"

    def _assert_plan_has_federated_scan(self, sql: str):
        tdSql.query(f"explain verbose true {sql}")
        rows = self._rows()
        assert rows, f"EXPLAIN VERBOSE returned no rows: {sql}"
        text = "\n".join(str(cell) for row in rows for cell in row)
        assert "federated" in text.lower(), f"plan lacks federated scan: {text}"

    def _assert_explain_analyze_has_federated_scan(self, sql: str):
        tdSql.query(f"explain analyze {sql}", queryTimes=1)
        rows = self._rows()
        assert rows, f"EXPLAIN ANALYZE returned no rows: {sql}"
        text = "\n".join(str(cell) for row in rows for cell in row)
        assert "federated" in text.lower(), f"analyze plan lacks federated scan: {text}"

    def _assert_remote_sql(self, sql: str, required_fragments: Sequence[str],
                           required_plan_fragments: Sequence[str] = (),
                           forbidden_remote_fragments: Sequence[str] = ()):
        tdSql.query(f"explain verbose true {sql}")
        lines = tuple(
            str(cell) for row in (tdSql.queryResult or ()) for cell in row if cell is not None
        )
        assert lines, f"EXPLAIN VERBOSE returned no rows: {sql}"
        remote_lines = tuple(line for line in lines if "remote sql:" in line.lower())
        assert remote_lines, f"plan has no remote SQL: sql={sql!r}, plan={lines!r}"
        remote_sql = "\n".join(remote_lines).lower()
        for fragment in required_fragments:
            assert fragment.lower() in remote_sql, (
                f"remote SQL misses {fragment!r}: sql={sql!r}, remote={remote_lines!r}"
            )
        for fragment in forbidden_remote_fragments:
            assert fragment.lower() not in remote_sql, (
                f"remote SQL unexpectedly has {fragment!r}: sql={sql!r}, remote={remote_lines!r}"
            )
        plan_text = "\n".join(lines).lower()
        for fragment in required_plan_fragments:
            assert fragment.lower() in plan_text, (
                f"plan misses {fragment!r}: sql={sql!r}, plan={lines!r}"
            )

    # --- DDL identifier and source metadata boundaries ---

    @staticmethod
    def _utf8_exact_bytes(total: int, suffix: str = "") -> str:
        suffix_len = len(suffix.encode("utf-8"))
        assert suffix_len <= total
        value = "a" * (total - suffix_len) + suffix
        assert len(value.encode("utf-8")) == total
        return value

    def _assert_no_partial_source(self, name: str):
        self._assert_source_absent(name)
        self._assert_rows("select 1", ((1,),))

    def do_source_identifier_boundaries(self):
        variants = (
            (63, ""), (64, ""), (64, "é"), (64, "中"), (64, "🙂"), (65, ""),
        )
        for spec in self.specs.values():
            for length, suffix in variants:
                name = self._utf8_exact_bytes(length, suffix)
                self._drop_source(name)
                sql = self._source_sql(spec, name=name, schema="public")
                if length <= 64:
                    tdSql.execute(sql)
                    self._assert_source_exists(name)
                    self._drop_source(name)
                else:
                    self._assert_error(
                        sql, TSDB_CODE_EXT_IDENTIFIER_TOO_LONG, "too long"
                    )
                    self._assert_no_partial_source(name)

            reserved = "select"
            self._drop_source(reserved)
            tdSql.execute(self._source_sql(spec, name=reserved, schema="public"))
            self._assert_source_exists(reserved)
            self._drop_source(reserved)
        print("FQ07 source identifier boundaries ................ [ passed ]")

    def do_database_schema_alter_boundaries(self):
        values = (
            (63, self._utf8_exact_bytes(63)),
            (64, self._utf8_exact_bytes(64, "中")),
            (65, self._utf8_exact_bytes(65, "🙂")),
        )
        for spec in self.specs.values():
            name = f"fq07_ddl_{spec.key[0]}"
            self._drop_source(name)
            for length, database in values:
                sql = self._source_sql(spec, name=name, database=database, schema="public")
                if length <= 64:
                    tdSql.execute(sql)
                    self._assert_source_exists(name)
                    self._assert_source_config_value(name, "database", database)
                    self._drop_source(name)
                else:
                    self._assert_error(sql, TSDB_CODE_EXT_IDENTIFIER_TOO_LONG, "too long")
                    self._assert_no_partial_source(name)

            tdSql.execute(self._source_sql(spec, name=name, schema="public"))
            self._assert_source_exists(name)
            for length, database in values:
                alter = (f"alter external source {self._source_ref(name)} "
                         f"set database='{database}'")
                if length <= 64:
                    tdSql.execute(alter)
                    self._assert_source_config_value(name, "database", database)
                else:
                    before = self._rows_after_describe(name)
                    self._assert_error(alter, TSDB_CODE_EXT_IDENTIFIER_TOO_LONG, "too long")
                    assert self._rows_after_describe(name) == before, "failed ALTER changed source"
            self._drop_source(name)

        pg = self.specs[_PG]
        name = "fq07_ddl_schema"
        self._drop_source(name)
        tdSql.execute(self._source_sql(pg, name=name, schema="public"))
        self._assert_source_exists(name)
        for length, schema in values:
            alter = (f"alter external source {self._source_ref(name)} "
                     f"set schema='{schema}'")
            if length <= 64:
                tdSql.execute(alter)
                self._assert_source_config_value(name, "schema", schema)
            else:
                before = self._rows_after_describe(name)
                self._assert_error(alter, TSDB_CODE_EXT_IDENTIFIER_TOO_LONG, "too long")
                assert self._rows_after_describe(name) == before, "failed schema ALTER changed source"
        self._drop_source(name)
        print("FQ07 database/schema boundaries ................. [ passed ]")

    def _rows_after_describe(self, name: str) -> Tuple[Tuple[object, ...], ...]:
        tdSql.query(
            "select source_name, `type`, host, `port`, `user`, password, `database`, "
            "`schema`, `options` from information_schema.ins_ext_sources "
            f"where source_name = '{name}'"
        )
        rows = tuple(self._rows())
        assert rows, f"DESCRIBE returned no rows for {name}"
        return rows

    def do_options_and_masking_boundaries(self):
        long_key = "k" * 65
        secret = "fq07_secret_must_not_escape"
        for spec in self.specs.values():
            name = f"fq07_opts_{spec.key[0]}"
            self._drop_source(name)
            if spec.key == _INFLUX:
                options = f"'api_token'='{secret}','protocol'='flight_sql'"
            else:
                options = "'connect_timeout_ms'='1000'"
            source_schema = "dbo" if spec.key == _SQLSERVER else "public"
            tdSql.execute(self._source_sql(spec, name=name, schema=source_schema, options=options))
            self._assert_source_exists(name)
            self._assert_error(
                f"alter external source {self._source_ref(name)} "
                f"set options('{long_key}'='x')",
                TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG,
                "too long",
                (secret,),
            )
            described = self._rows_after_describe(name)
            described_text = "\n".join(str(cell) for row in described for cell in row)
            assert long_key not in described_text
            assert secret not in described_text, "sensitive option leaked by DESCRIBE"
            self._drop_source(name)
        print("FQ07 options and masking boundaries ............. [ passed ]")

    # --- schema, delayed type errors, and recovery ---

    def do_schema_normalization(self):
        expected = {
            _MYSQL: (
                ("ts", "TIMESTAMP", 8, ""), ("c_char", "NCHAR", 4, ""),
                ("c_varchar", "NCHAR", 8, ""), ("c_bin", "VARBINARY", 8, ""),
                ("c_blob", "VARBINARY", 65517, ""), ("c_json", "NCHAR", 16379, ""),
                ("c_dec", "DECIMAL(18, 4)", 8, ""), ("c_geom", "GEOMETRY", 65517, ""),
            ),
            _PG: (
                ("ts", "TIMESTAMP", 8, ""), ("c_char", "NCHAR", 4, ""),
                ("c_varchar", "NCHAR", 8, ""), ("c_bin", "VARBINARY", 65517, ""),
                ("c_json", "NCHAR", 16379, ""), ("c_xml", "NCHAR", 16379, ""),
                ("c_array", "NCHAR", 16379, ""), ("c_dec", "DECIMAL(18, 4)", 8, ""),
            ),
            _SQLSERVER: (
                # SQL Server char/varchar are non-Unicode columns and map to
                # VARCHAR; only nchar/nvarchar map to NCHAR (unlike MySQL, whose
                # utf8mb4 text columns are Unicode and map to NCHAR).
                ("ts", "TIMESTAMP", 8, ""), ("c_char", "VARCHAR", 4, ""),
                ("c_varchar", "VARCHAR", 8, ""), ("c_bin", "VARBINARY", 8, ""),
                ("c_json", "NCHAR", 16379, ""), ("c_dec", "DECIMAL(18, 4)", 8, ""),
                ("c_geom", "GEOMETRY", 65517, ""),
            ),
            _ORACLE: (
                # Oracle reports object names exactly as stored in the data
                # dictionary (upper case for unquoted DDL), and VARCHAR2/CHAR
                # map to VARCHAR while CLOB maps to NCHAR (DS 5.3.2.1).
                ("TS", "TIMESTAMP", 8, ""), ("C_CHAR", "VARCHAR", 4, ""),
                ("C_VARCHAR", "VARCHAR", 8, ""), ("C_BIN", "VARBINARY", 8, ""),
                ("C_CLOB", "NCHAR", 16379, ""), ("C_DEC", "DECIMAL(18, 4)", 8, ""),
            ),
            _INFLUX: (
                ("time", "TIMESTAMP", 8, ""), ("c_float", "DOUBLE", 8, ""),
                ("c_int", "BIGINT", 8, ""), ("c_str", "NCHAR", 16379, ""),
                ("c_bool", "BOOL", 1, ""),
            ),
        }
        for source_key, schema in expected.items():
            spec = self.specs[source_key]
            self._assert_rows(f"describe {spec.source}.schema_t", schema)
        print("FQ07 schema normalization ....................... [ passed ]")

    def do_client_field_metadata(self):
        common = (
            FieldSchema("id", FieldType.C_INT, 0, 0, 4),
            FieldSchema("score", FieldType.C_DOUBLE, 0, 0, 8),
            FieldSchema("name", FieldType.C_NCHAR, 0, 0, 32),
            FieldSchema("flag", FieldType.C_BOOL, 0, 0, 1),
        )
        expected = {
            _MYSQL: common + (FieldSchema("note", FieldType.C_NCHAR, 0, 0, 32),),
            _PG: common + (FieldSchema("note", FieldType.C_NCHAR, 0, 0, 16379),),
            _SQLSERVER: common + (FieldSchema("note", FieldType.C_NCHAR, 0, 0, 32),),
            _ORACLE: (
                # The result column names are the names the query wrote, not the
                # remote data-dictionary spelling; the types come from the type
                # map (NUMBER(10,0) -> BIGINT, VARCHAR2 -> VARCHAR,
                # NUMBER(1,0) -> TINYINT, DS 5.3.2.1).
                FieldSchema("id", FieldType.C_BIGINT, 0, 0, 8),
                FieldSchema("score", FieldType.C_DOUBLE, 0, 0, 8),
                FieldSchema("name", FieldType.C_VARCHAR, 0, 0, 32),
                FieldSchema("flag", FieldType.C_TINYINT, 0, 0, 1),
                FieldSchema("note", FieldType.C_VARCHAR, 0, 0, 32),
            ),
            _INFLUX: (
                FieldSchema("id", FieldType.C_BIGINT, 0, 0, 8),
                FieldSchema("score", FieldType.C_DOUBLE, 0, 0, 8),
                FieldSchema("name", FieldType.C_NCHAR, 0, 0, 16379),
                FieldSchema("flag", FieldType.C_BOOL, 0, 0, 1),
                FieldSchema("note", FieldType.C_NCHAR, 0, 0, 16379),
            ),
        }
        oracle_rows = ((1, 1.5, "alpha", 1, None),)
        for source_key, fields in expected.items():
            spec = self.specs[source_key]
            self._assert_field_schema(
                f"select id, score, name, flag, note from {spec.table} where id = 1",
                fields,
                oracle_rows if source_key == _ORACLE else ((1, 1.5, "alpha", True, ""),),
            )

        decimal_fields = {
            _MYSQL: (FieldSchema("dec_ok", FieldType.C_DECIMAL64, 18, 4, 64),),
            _PG: (FieldSchema("dec_ok", FieldType.C_DECIMAL64, 18, 4, 64),),
            _ORACLE: (FieldSchema("dec_ok", FieldType.C_DECIMAL64, 18, 4, 64),),
            _INFLUX: (FieldSchema("dec_ok", FieldType.C_DOUBLE, 0, 0, 8),),
        }
        for source_key, fields in decimal_fields.items():
            spec = self.specs[source_key]
            self._assert_field_schema(
                f"select dec_ok from {spec.table} where id = 1",
                fields,
                (
                    ((Decimal("123.4567"),),)
                    if source_key != _INFLUX
                    else ((123.4567,),)
                ),
            )
        print("FQ07 TAOS_FIELD_E metadata ...................... [ passed ]")

    def do_value_conversion_boundaries(self):
        expected_by_source = {
            _SQLSERVER: ((1, _SQLSERVER_NCHAR_VALUES[0]), (2, _SQLSERVER_NCHAR_VALUES[1])),
        }
        for spec in self.specs.values():
            expected = expected_by_source.get(
                spec.key, ((1, _NCHAR_VALUES[0]), (2, _NCHAR_VALUES[1])))
            table = f"{spec.source}.value_t"
            sql = f"select id as id, note as note from {table} where id < 3 order by id"
            self._assert_rows(sql, expected)
            self._assert_result_columns(sql, ("id", "note"))
            if spec.key == _INFLUX:
                # InfluxDB strings are unbounded, so there is no fixed-width
                # overflow to detect.
                self._assert_rows("select 1", ((1,),))
                continue
            # SQL Server keeps the over-budget value in value_big_t: it is
            # zero-pushdown, so the scan converts the whole block before the
            # local filter runs and a single over-budget row would fail every
            # query on value_t (not just the one selecting that row).
            overflow_table = (f"{spec.source}.value_big_t"
                              if spec.key == _SQLSERVER else table)
            self._assert_error(
                f"select id, note from {overflow_table} where id = 3",
                TSDB_CODE_EXT_TYPE_CONVERT_FAILED,
                "conversion failed",
            )
            self._assert_rows(sql, expected)
            self._assert_result_columns(sql, ("id", "note"))
        print("FQ07 value conversion boundaries ............... [ passed ]")

    def do_unknown_type_delayed_errors(self):
        queries = (
            "select shape from {table}",
            "select * from {table}",
            "select id from {table} where shape is not null",
            "select id from {table} order by shape",
            "select distinct shape from {table}",
            "select case when shape is null then 0 else 1 end from {table}",
            "select cast(shape as varchar(16)) from {table}",
            "select id from (select id, shape from {table}) q",
            "select id from {table} union all select id from {table} where shape is null",
        )
        for source_key in (_MYSQL, _PG, _SQLSERVER, _ORACLE):
            spec = self.specs[source_key]
            table = f"{spec.source}.unknown_t"
            self._assert_rows(f"select id from {table} order by id", ((1,),))
            for query in queries:
                self._assert_error(
                    query.format(table=table), TSDB_CODE_EXT_TYPE_NOT_MAPPABLE,
                    "cannot be mapped",
                )
        print("FQ07 delayed unknown-type errors ............... [ passed ]")

    def do_error_recovery_and_metadata_refresh(self):
        for spec in self.specs.values():
            self._assert_error(
                f"select no_such_column from {spec.table}",
                _code("TSDB_CODE_PAR_INVALID_COLUMN"),
            )
            tdSql.execute(f"refresh external source {self._source_ref(spec.source)}")
            self._assert_rows(
                f"select count(*) from {spec.table}", ((3,),)
            )
        print("FQ07 recovery and metadata refresh ............. [ passed ]")

    def do_pool_reuse_and_refresh_matrix(self):
        expected = ((1, 10), (2, 20), (3, None))
        for spec in self.specs.values():
            sql = f"select id, val from {spec.table} order by id"
            for _ in range(3):
                self._assert_rows(sql, expected)
                self._assert_result_columns(sql, ("id", "val"))
                tdSql.execute(f"refresh external source {self._source_ref(spec.source)}")
                self._assert_rows(sql, expected)
                self._assert_result_columns(sql, ("id", "val"))
                self._assert_rows("select 1", ((1,),))
        print("FQ07 pool reuse and repeated refresh ........... [ passed ]")

    def do_concurrent_metadata_refresh_matrix(self):
        expected = ((1, 10), (2, 20), (3, None))
        for spec in self.specs.values():
            sql = f"select id, val from {spec.table} order by id"
            barrier = threading.Barrier(3)
            errors = []
            errors_lock = threading.Lock()

            def query_worker():
                import taos

                conn = None
                cursor = None
                try:
                    conn = taos.connect()
                    cursor = conn.cursor()
                    barrier.wait(timeout=15)
                    for _ in range(3):
                        cursor.execute(sql)
                        rows = tuple(tuple(row) for row in cursor.fetchall())
                        if rows != expected:
                            raise AssertionError(
                                f"concurrent query result mismatch: expected={expected!r}, actual={rows!r}"
                            )
                except Exception as exc:
                    with errors_lock:
                        errors.append(repr(exc))
                finally:
                    if cursor is not None:
                        cursor.close()
                    if conn is not None:
                        conn.close()

            workers = [threading.Thread(target=query_worker, daemon=True) for _ in range(2)]
            for worker in workers:
                worker.start()
            try:
                barrier.wait(timeout=15)
                for _ in range(3):
                    tdSql.execute(f"refresh external source {self._source_ref(spec.source)}")
                    self._assert_rows(sql, expected)
                    self._assert_result_columns(sql, ("id", "val"))
            finally:
                for worker in workers:
                    worker.join(timeout=30)
            assert all(not worker.is_alive() for worker in workers), (
                f"concurrent refresh workers did not exit for {spec.key}"
            )
            assert not errors, f"concurrent refresh/query failures for {spec.key}: {errors!r}"
            self._assert_rows("select 1", ((1,),))
        print("FQ07 concurrent metadata refresh matrix ........ [ passed ]")

    def do_partition_by_tbname_matrix(self):
        for source_key, spec in self.specs.items():
            sql = f"select count(*) as total from {spec.table} partition by tbname order by total"
            if source_key == _INFLUX:
                self._assert_rows(sql, ((1,), (2,)))
                self._assert_result_columns(sql, ("total",))
            else:
                self._assert_error(sql, TSDB_CODE_EXT_SYNTAX_UNSUPPORTED)
        print("FQ07 partition-by-tbname matrix ............... [ passed ]")

    def do_execution_mode_and_remote_sql_matrix(self):
        for spec in self.specs.values():
            direct_sql = (
                f"select id as id, val as val from {spec.table} "
                "where val >= 10 order by id limit 2"
            )
            self._assert_rows(direct_sql, ((1, 10), (2, 20)))
            self._assert_result_columns(direct_sql, ("id", "val"))
            # T-SQL renders LIMIT as OFFSET/FETCH and Oracle renders it as
            # FETCH FIRST ... ROWS ONLY (neither emits the "limit" keyword).
            if spec.key == _SQLSERVER:
                limit_fragment = ("offset", "fetch next")
            elif spec.key == _ORACLE:
                limit_fragment = ("fetch first", "rows only")
            else:
                limit_fragment = ("limit",)
            self._assert_remote_sql(
                direct_sql,
                ("select", "where", "order by", *limit_fragment),
                ("federated",),
            )

            partial_sql = (
                f"select e.id as id, d.weight as weight from {spec.table} e "
                f"join {_LOCAL_DB}.dim d on e.{spec.timestamp_column} = d.ts "
                "order by e.id"
            )
            self._assert_rows(partial_sql, ((1, 100), (2, 200)))
            self._assert_result_columns(partial_sql, ("id", "weight"))
            self._assert_remote_sql(partial_sql, ("select",), ("federated", "join"))

            remote_filter_join_sql = (
                f"select e.id as id, d.weight as weight from {spec.table} e "
                f"join {_LOCAL_DB}.dim d on e.{spec.timestamp_column} = d.ts "
                "where e.val >= 10 order by e.id"
            )
            self._assert_rows(remote_filter_join_sql, ((1, 100), (2, 200)))
            self._assert_result_columns(remote_filter_join_sql, ("id", "weight"))
            self._assert_remote_sql(
                remote_filter_join_sql, ("select", "where"), ("federated", "join")
            )

            local_filter_join_sql = (
                f"select e.id as id, d.weight as weight from {spec.table} e "
                f"join {_LOCAL_DB}.dim d on e.{spec.timestamp_column} = d.ts "
                "where d.weight >= 100 order by e.id"
            )
            self._assert_rows(local_filter_join_sql, ((1, 100), (2, 200)))
            self._assert_result_columns(local_filter_join_sql, ("id", "weight"))
            self._assert_remote_sql(local_filter_join_sql, ("select",), ("federated", "join"))

            self._assert_rows("select 1", ((1,),))
        print("FQ07 execution mode and remote SQL matrix ...... [ passed ]")

    # --- coverage and assertion audit ---

    def do_coverage_and_assertion_audit(self):
        required_sections = {"6.1", "6.3", "6.4", "6.8"}
        covered_sections = {section for case in _QUERY_CASES for section in case.coverage}
        missing = required_sections - covered_sections
        assert not missing, f"query matrix misses coverage sections: {sorted(missing)}"
        for case in _QUERY_CASES:
            assert case.expected_rows is not None, f"{case.case_id} has no result assertion"
            assert set(case.sources) == set(_ALL_SOURCES), (
                f"{case.case_id} does not cover every real external library"
            )
            assert case.sql_template.strip().lower().startswith("select"), (
                f"{case.case_id} must be an asserted query"
            )
        assert TSDB_CODE_EXT_REQUIRED_COLUMNS_TOO_MANY is not None
        assert TSDB_CODE_EXT_REQUIRED_TAGS_TOO_MANY is not None
        assert TSDB_CODE_EXT_TYPE_CONVERT_FAILED is not None
        print("FQ07 coverage and assertion audit .............. [ passed ]")

    def test_fq_07_boundary_contracts(self):
        """Federated query boundary contracts.

        1. Validate external-source DDL identifiers, ALTER rollback, options and masking.
        2. Validate five-library path, result, schema, EXPLAIN and SQL-shape matrices.
        3. Validate delayed unknown-type errors, recovery and assertion coverage.

        Catalog:
            - FederatedQuery

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-07-23 wpan Add federated boundary contracts

        """
        try:
            self.do_source_identifier_boundaries()
            self.do_database_schema_alter_boundaries()
            self.do_options_and_masking_boundaries()
            self.do_query_result_matrix()
            self.do_influx_http_parity()
            self.do_native_fetch_batch_matrix()
            self.do_native_structured_and_decimal_matrix()
            self.do_path_and_explain_matrix()
            self.do_use_context_matrix()
            self.do_schema_normalization()
            self.do_client_field_metadata()
            self.do_value_conversion_boundaries()
            self.do_unknown_type_delayed_errors()
            self.do_error_recovery_and_metadata_refresh()
            self.do_pool_reuse_and_refresh_matrix()
            self.do_concurrent_metadata_refresh_matrix()
            self.do_partition_by_tbname_matrix()
            self.do_execution_mode_and_remote_sql_matrix()
            self.do_coverage_and_assertion_audit()
        finally:
            self._cleanup_runtime_data()
