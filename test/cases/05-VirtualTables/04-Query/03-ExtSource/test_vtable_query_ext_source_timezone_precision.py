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

# -*- coding: utf-8 -*-
"""Virtual-table time type / precision / timezone matrix (FQVT-TTP-*).

The virtual-table suites used to cover precision only incidentally: one
Oracle case read a TIMESTAMP(9) *primary key* and one pushdown case carried
non-key timestamp columns from a table whose finest declaration was
TIMESTAMP(6).  Neither can observe a value that is decoded at microsecond
transport precision, because the only extra digits that path drops are the
sub-microsecond ones.

This suite drives the full matrix on purpose:

  FQVT-TTP-001  every time type of every external source as the virtual
                table's time axis (primary key), at every declared precision
  FQVT-TTP-002  every time type of every external source as a NON-key
                column, read from ms / us / ns virtual tables
  FQVT-TTP-003  the same values read through the external table directly,
                so the virtual-table path must agree with the direct path
  FQVT-TTP-004  a virtual table whose precision is lower than the referenced
                time axis must be rejected with 0x268f, for every source and
                every precision step (ms -> us, ms -> ns, us -> ns)
  FQVT-TTP-005  tz-aware columns keep one instant across client timezones
                while tz-naive columns follow the client timezone
  FQVT-TTP-006  time-range predicates on the virtual time axis select the
                same rows across client timezones and at exact boundaries
  FQVT-TTP-007  the virtual-super-table (USING) path applies the same
                precision contract and reads non-key columns correctly

Catalog:
    - VirtualTable

Since: v3.5.0.0

Labels: common,ci

Jira: None

History:
    - 2026-11-05 wpan Add the virtual-table precision/timezone matrix

"""

import os
import sys
import time

import pytest

from new_test_framework.utils import tdLog, tdSql

_FQ_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..",
                 "09-DataQuerying", "19-FederatedQuery"))
if _FQ_DIR not in sys.path:
    sys.path.insert(0, _FQ_DIR)

from federated_time_types import (  # noqa: E402
    BACKENDS,
    INFLUX,
    MYSQL,
    NS_PER_RANK,
    PG,
    RANK_INDEX,
    RANKS,
    ROWS,
    SQLSERVER,
    ORACLE,
    INFLUX_TYPES,
    axis_types,
    expected_direct_value,
    expected_value,
    find_type,
    mixed_pk as _mixed_pk_type,
    mixed_types,
)
from ext_source_helpers import ExtSrcEnv  # noqa: E402

_LOCAL_DBS = {
    "ms": "vt_ttp_ms",
    "us": "vt_ttp_us",
    "ns": "vt_ttp_ns",
}
_VSTABLE_DB = "vt_ttp_us"
_VSTABLE = "vt_ttp_stb"

_DEFAULT_TZ = "Asia/Shanghai"
_TZ_LIST = ("Asia/Shanghai", "UTC", "America/New_York")

_PRECISION_ERRNO = 0x268f

# InfluxDB 3 serves unflushed WAL data to its HTTP SQL API immediately, but a
# FlightSQL query (the protocol the connector uses) only sees data once the
# background persister has written it to Parquet.  A measurement written just
# before the assertion is therefore invisible for a while; poll instead of
# asserting on the first empty block.
_INFLUX_PERSIST_TIMEOUT_S = 180

# One representative time type per precision class is enough for the
# ms->us / ms->ns / us->ns upscale checks: the mixed-column matrix already
# covers every type at every scale.
_SCALE_AXIS_LABELS = ("datetime3", "datetime6", "timestamp3", "timestamp6",
                      "timestamp9", "tstltz3", "tstltz9",
                      "datetime2_3", "datetime2_6", "datetimeoffset_3",
                      "datetimeoffset_6")


def _axis_table(backend, ts_type):
    """Remote axis table / local vtable stem, unique per backend.

    Several backends expose the same type labels (date, timestamp3, ...), and
    every virtual table of the suite lives in one of three local databases, so
    the name has to carry the backend key.
    """
    return "ax_%s_%s" % (backend.key, ts_type.label)


def _mixed_table(backend):
    return "mix_" + backend.key


def _mixed_ns_table(backend):
    return "mix_ns_" + backend.key


def _find_type(backend, label):
    try:
        return find_type(backend, label)
    except KeyError:
        raise


def _mixed_pk(backend):
    """Primary key of the mixed-column table: a millisecond-class axis."""
    return _mixed_pk_type(backend)


class TestVtableQueryExtSourceTimezonePrecision:
    """Time type / precision / timezone matrix for external-column vtables."""

    updatecfgDict = {
        "federatedQueryEnable": 1,
        "timezone": _DEFAULT_TZ,
        "clientCfg": {
            "federatedQueryEnable": 1,
            "timezone": _DEFAULT_TZ,
        },
    }

    # --- fixtures recorded for the assertions ---------------------------
    _axis_tables = {}
    _mixed_tables = {}
    _axis_vts = []
    _mixed_vts = []

    # ------------------------------------------------------------------
    # setup / teardown
    # ------------------------------------------------------------------
    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        ExtSrcEnv.ensure_env()
        ExtSrcEnv.ensure_qnode()

        cls._axis_tables = {backend.key: [] for backend in BACKENDS}
        cls._mixed_tables = {backend.key: [] for backend in BACKENDS}
        cls._axis_vts = []
        cls._mixed_vts = []

        for precision, database in _LOCAL_DBS.items():
            tdSql.execute(f"DROP DATABASE IF EXISTS {database}")
            tdSql.execute(f"CREATE DATABASE {database} PRECISION '{precision}'")
        tdSql.execute(f"DROP DATABASE IF EXISTS {_VSTABLE_DB}")
        tdSql.execute(f"CREATE DATABASE {_VSTABLE_DB} PRECISION 'us'")

        for backend in BACKENDS:
            backend.prepare()
            cls._prepare_fixtures(backend)

        cls._create_axis_vtables()
        cls._create_mixed_vtables()

    @classmethod
    def teardown_class(cls):
        for database in list(_LOCAL_DBS.values()) + [_VSTABLE_DB]:
            try:
                tdSql.execute(f"DROP DATABASE IF EXISTS {database}")
            except Exception as exc:  # noqa: BLE001 - best-effort teardown
                tdLog.info(f"drop database {database}: {exc}")
        for backend in BACKENDS:
            try:
                tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {backend.source}")
            except Exception as exc:  # noqa: BLE001 - best-effort teardown
                tdLog.info(f"drop source {backend.source}: {exc}")
            try:
                names = [table for table, _ in cls._axis_tables[backend.key]]
                names += [table for table, _, _ in cls._mixed_tables[backend.key]]
                backend.cleanup(names)
            except Exception as exc:  # noqa: BLE001 - best-effort teardown
                tdLog.info(f"cleanup {backend.key}: {exc}")

    @classmethod
    def _prepare_fixtures(cls, backend):
        if backend.key == INFLUX.key:
            # A measurement has exactly one time column, so it can only cover
            # the time-axis matrix.
            measurement = "ax_" + backend.key + "_time"
            backend.create_measurement(measurement)
            backend.wait_visible(measurement)
            cls._axis_tables[backend.key].append((measurement, None))
            return

        for ts_type in axis_types(backend):
            table = _axis_table(backend, ts_type)
            backend.create_axis_table(table, ts_type)
            cls._axis_tables[backend.key].append((table, ts_type))

        pk_type = _mixed_pk(backend)
        cols = mixed_types(backend)
        backend.create_mixed_table(_mixed_table(backend), pk_type, cols)
        cls._mixed_tables[backend.key].append(
            (_mixed_table(backend), pk_type, cols))

        if backend.key == ORACLE.key:
            # The reverse shape: the time axis itself is nanosecond while the
            # remaining columns are coarser.
            ns_pk = _find_type(backend, "timestamp9")
            backend.create_mixed_table(_mixed_ns_table(backend), ns_pk, cols)
            cls._mixed_tables[backend.key].append(
                (_mixed_ns_table(backend), ns_pk, cols))

    @classmethod
    def _create_axis_vtables(cls):
        for backend in BACKENDS:
            for table, ts_type in cls._axis_tables[backend.key]:
                if ts_type is None:
                    dst = "ns"
                else:
                    dst = ts_type.rank
                vt = "v_" + table
                tdSql.execute(
                    f"CREATE VTABLE {_LOCAL_DBS[dst]}.{vt} "
                    f"(ts timestamp, v {backend.v_decl} "
                    f"FROM {backend.col_ref(table, 'v')})")
                cls._axis_vts.append({
                    "backend": backend,
                    "table": table,
                    "vt": vt,
                    "db": dst,
                    "type": ts_type,
                    "transport": backend.transport_rank(
                        [ts_type] if ts_type is not None else []),
                })

            # Upscale coverage for the representative axes: a value read at a
            # finer destination precision must keep every declared digit.
            for label in _SCALE_AXIS_LABELS:
                try:
                    ts_type = _find_type(backend, label)
                except KeyError:
                    continue
                if not ts_type.pk_ok:
                    continue
                table = _axis_table(backend, ts_type)
                for dst in RANKS[RANK_INDEX[ts_type.rank] + 1:]:
                    vt = f"v_{table}_in_{dst}"
                    tdSql.execute(
                        f"CREATE VTABLE {_LOCAL_DBS[dst]}.{vt} "
                        f"(ts timestamp, v {backend.v_decl} "
                        f"FROM {backend.col_ref(table, 'v')})")
                    cls._axis_vts.append({
                        "backend": backend,
                        "table": table,
                        "vt": vt,
                        "db": dst,
                        "type": ts_type,
                        "transport": backend.transport_rank([ts_type]),
                    })

    @classmethod
    def _create_mixed_vtables(cls):
        for backend in BACKENDS:
            for table, pk_type, cols in cls._mixed_tables[backend.key]:
                transport = backend.transport_rank([pk_type] + list(cols))
                refs = ", ".join(
                    f"{t.col} timestamp FROM {backend.col_ref(table, t.col)}"
                    for t in cols)
                for dst in RANKS[RANK_INDEX[pk_type.rank]:]:
                    vt = f"v_{table}_{dst}"
                    tdSql.execute(
                        f"CREATE VTABLE {_LOCAL_DBS[dst]}.{vt} "
                        f"(ts timestamp, {refs}, "
                        f"v {backend.v_decl} FROM {backend.col_ref(table, 'v')})")
                    cls._mixed_vts.append({
                        "backend": backend,
                        "table": table,
                        "vt": vt,
                        "db": dst,
                        "pk": pk_type,
                        "cols": cols,
                        "transport": transport,
                    })

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    @staticmethod
    @staticmethod
    def _wait_influx_rows(sql):
        deadline = time.monotonic() + _INFLUX_PERSIST_TIMEOUT_S
        while True:
            tdSql.query(sql)
            if tdSql.queryRows > 0:
                return
            if time.monotonic() >= deadline:
                tdLog.exit(
                    "InfluxDB measurement is still invisible to FlightSQL after "
                    f"{_INFLUX_PERSIST_TIMEOUT_S}s (WAL not persisted yet): {sql}")
            time.sleep(5)

    @staticmethod
    def _use_timezone(timezone_name):
        tdSql.connect("root", "taosdata")
        tdSql.execute('ALTER LOCAL "federatedQueryEnable" "1"')
        tdSql.execute(f'ALTER LOCAL "timezone" "{timezone_name}"')

    # ------------------------------------------------------------------
    # FQVT-TTP-001: every time type as the virtual time axis
    # ------------------------------------------------------------------
    def do_axis_matrix(self):
        self._use_timezone(_DEFAULT_TZ)
        for entry in self._axis_vts:
            ts_type = entry["type"]
            if ts_type is None:
                # InfluxDB has its own case (test_vtable_influx_time_axis):
                # its FlightSQL view depends on a background WAL snapshot and
                # needs a diagnostic when the data is not there yet.
                continue

            transport = entry["transport"]
            tz = _DEFAULT_TZ
            tdSql.query(
                f"SELECT CAST(ts AS BIGINT), v "
                f"FROM {_LOCAL_DBS[entry['db']]}.{entry['vt']} ORDER BY ts")
            tdSql.checkRows(len(ROWS))
            for index, row in enumerate(ROWS):
                tdSql.checkData(
                    index, 0,
                    expected_value(row, ts_type, tz, transport, entry["db"]))
                tdSql.checkData(index, 1, row.v)
        print("vtable time-axis matrix ................. [ passed ]")

    # ------------------------------------------------------------------
    # FQVT-TTP-002: every time type as a non-key column
    # ------------------------------------------------------------------
    def do_non_key_matrix(self):
        self._use_timezone(_DEFAULT_TZ)
        for entry in self._mixed_vts:
            cols = entry["cols"]
            select = ", ".join(f"CAST({t.col} AS BIGINT)" for t in cols)
            tdSql.query(
                f"SELECT {select}, v "
                f"FROM {_LOCAL_DBS[entry['db']]}.{entry['vt']} ORDER BY ts")
            tdSql.checkRows(len(ROWS))
            for row_index, row in enumerate(ROWS):
                for col_index, ts_type in enumerate(cols):
                    tdSql.checkData(
                        row_index, col_index,
                        expected_value(row, ts_type, _DEFAULT_TZ,
                                       entry["transport"], entry["db"]))
                tdSql.checkData(row_index, len(cols), row.v)
        print("vtable non-key time columns ............. [ passed ]")

    # ------------------------------------------------------------------
    # FQVT-TTP-003: virtual-table read == direct external-table read
    # ------------------------------------------------------------------
    def do_direct_parity(self):
        self._use_timezone(_DEFAULT_TZ)
        for backend in BACKENDS:
            if backend.key == INFLUX.key:
                continue
            for table, pk_type, cols in self._mixed_tables[backend.key]:
                transport = backend.transport_rank([pk_type] + list(cols))
                select = ", ".join(f"CAST({t.col} AS BIGINT)" for t in cols)
                tdSql.query(
                    f"SELECT {select}, v "
                    f"FROM {backend.direct_table_ref(table)} ORDER BY ts")
                tdSql.checkRows(len(ROWS))
                direct = [list(row) for row in tdSql.queryResult]

                vtable = next(v for v in self._mixed_vts
                              if v["backend"].key == backend.key
                              and v["table"] == table and v["db"] == "ns")
                tdSql.query(
                    f"SELECT {select}, v "
                    f"FROM {_LOCAL_DBS['ns']}.{vtable['vt']} ORDER BY ts")
                tdSql.checkRows(len(ROWS))
                through_vtable = [list(row) for row in tdSql.queryResult]

                factor = NS_PER_RANK[transport]
                for row_index in range(len(ROWS)):
                    for col_index, ts_type in enumerate(cols):
                        expected_direct = expected_direct_value(
                            ROWS[row_index], ts_type, _DEFAULT_TZ, transport)
                        assert direct[row_index][col_index] == expected_direct, (
                            backend.key, table, ts_type.label, row_index,
                            direct[row_index][col_index], expected_direct)
                        assert through_vtable[row_index][col_index] == (
                            direct[row_index][col_index] * factor), (
                            backend.key, table, ts_type.label, row_index,
                            through_vtable[row_index][col_index],
                            direct[row_index][col_index] * factor)
                    assert direct[row_index][len(cols)] == ROWS[row_index].v, (
                        backend.key, table, "v", row_index,
                        direct[row_index][len(cols)])
                    assert through_vtable[row_index][len(cols)] == ROWS[row_index].v, (
                        backend.key, table, "v", row_index,
                        through_vtable[row_index][len(cols)])
        print("direct vs vtable parity ................. [ passed ]")

    # ------------------------------------------------------------------
    # FQVT-TTP-004: lower virtual-table precision is rejected (0x268f)
    # ------------------------------------------------------------------
    def do_precision_negative(self):
        self._use_timezone(_DEFAULT_TZ)
        checked = 0
        for backend in BACKENDS:
            for table, ts_type in self._axis_tables[backend.key]:
                if ts_type is None:
                    source_rank = "ns"
                else:
                    source_rank = ts_type.rank
                for dst, database in _LOCAL_DBS.items():
                    if RANK_INDEX[dst] >= RANK_INDEX[source_rank]:
                        continue
                    sql = (f"CREATE VTABLE {database}.bad_{table}_{dst} "
                           f"(ts timestamp, v {backend.v_decl} "
                           f"FROM {backend.col_ref(table, 'v')})")
                    tdSql.error(sql, expectedErrno=_PRECISION_ERRNO)
                    checked += 1

        # The same contract on the virtual-super-table path: a child vtable
        # inherits the stable's precision and must reject a finer time axis.
        # The stable lives in the same database as its children, so the sweep
        # needs one stable per local precision.
        fine_axis = {}
        for backend, label, source_rank in (
                (MYSQL, "datetime6", "us"),
                (PG, "timestamp6", "us"),
                (SQLSERVER, "datetime2_6", "us"),
                (ORACLE, "timestamp9", "ns")):
            ts_type = _find_type(backend, label)
            fine_axis[backend.key] = (_axis_table(backend, ts_type), backend,
                                      source_rank, ts_type)
        for dst, database in _LOCAL_DBS.items():
            stable = _VSTABLE + "_" + dst
            tdSql.execute(f"DROP STABLE IF EXISTS {database}.{stable}")
            tdSql.execute(
                f"CREATE STABLE {database}.{stable} "
                f"(ts timestamp, c timestamp, v bigint) "
                f"TAGS (site nchar(16)) VIRTUAL 1")
            try:
                for backend_key, (table, backend, source_rank, ts_type) in \
                        fine_axis.items():
                    # `c` has to reference the remote time axis itself: the
                    # axis tables only carry that timestamp and an INT, and a
                    # TIMESTAMP declaration over the INT column would fail the
                    # type check before the precision check ever runs.
                    # Bind only the payload column: the time axis comes from
                    # the stable, and re-binding the remote axis column as a
                    # normal column is not a supported child-table shape.
                    sql = (
                        f"CREATE VTABLE {database}.ctb_prec_{backend_key} "
                        f"(v FROM {backend.col_ref(table, 'v')}) "
                        f"USING {database}.{stable} TAGS ('prec')")
                    if RANK_INDEX[dst] < RANK_INDEX[source_rank]:
                        tdSql.error(sql, expectedErrno=_PRECISION_ERRNO)
                        checked += 1
                        continue
                    tdSql.execute(sql)
                    tdSql.query(
                        f"SELECT CAST(ts AS BIGINT), v "
                        f"FROM {database}.ctb_prec_{backend_key} ORDER BY ts")
                    tdSql.checkRows(len(ROWS))
                    transport = backend.transport_rank([ts_type])
                    for row_index, row in enumerate(ROWS):
                        tdSql.checkData(
                            row_index, 0,
                            expected_value(row, ts_type, _DEFAULT_TZ,
                                           transport, dst))
                        tdSql.checkData(row_index, 1, row.v)
                    tdSql.execute(
                        f"DROP VTABLE {database}.ctb_prec_{backend_key}")
            finally:
                tdSql.execute(f"DROP STABLE IF EXISTS {database}.{stable}")

        assert checked >= 20, f"only {checked} negative precision cases ran"
        print("vtable precision rejection (%d) ...... [ passed ]" % checked)

    # ------------------------------------------------------------------
    # FQVT-TTP-005: timezone semantics of aware vs naive columns
    # ------------------------------------------------------------------
    def do_timezone_matrix(self):
        observed = {}
        try:
            for timezone_name in _TZ_LIST:
                self._use_timezone(timezone_name)
                for entry in self._mixed_vts:
                    if entry["db"] != "ns":
                        continue
                    cols = entry["cols"]
                    select = ", ".join(f"CAST({t.col} AS BIGINT)" for t in cols)
                    tdSql.query(
                        f"SELECT {select} "
                        f"FROM {_LOCAL_DBS['ns']}.{entry['vt']} ORDER BY ts")
                    tdSql.checkRows(len(ROWS))
                    for row_index, row in enumerate(ROWS):
                        for col_index, ts_type in enumerate(cols):
                            value = expected_value(row, ts_type, timezone_name,
                                                   entry["transport"], "ns")
                            tdSql.checkData(row_index, col_index, value)
                            if row_index == 0:
                                observed.setdefault(
                                    (entry["backend"].key, ts_type.label),
                                    {})[timezone_name] = value
        finally:
            self._use_timezone(_DEFAULT_TZ)

        # tz-aware columns pin the instant; tz-naive columns follow the
        # client timezone.  Compare the same column across the tz sweep.
        for (backend_key, label), per_tz in observed.items():
            ts_type = _find_type(
                next(b for b in BACKENDS if b.key == backend_key), label)
            if ts_type.aware:
                values = set(per_tz.values())
                assert len(values) == 1, (backend_key, label, per_tz)
            else:
                assert per_tz["Asia/Shanghai"] != per_tz["UTC"], (
                    backend_key, label, per_tz)
        print("vtable timezone matrix .................. [ passed ]")

    # ------------------------------------------------------------------
    # FQVT-TTP-006: range predicates on the virtual time axis
    # ------------------------------------------------------------------
    def do_time_range(self):
        day1 = ROWS[0].dt.strftime("%Y-%m-%d")
        day2 = ROWS[1].dt.strftime("%Y-%m-%d")
        try:
            for timezone_name in ("Asia/Shanghai", "UTC", "America/New_York"):
                self._use_timezone(timezone_name)
                for entry in self._mixed_vts:
                    if entry["db"] != "ns":
                        continue
                    qualified = f"{_LOCAL_DBS['ns']}.{entry['vt']}"

                    # Calendar-day bounds: row 0 sits on day1, row 1 on day2
                    # in the client timezone, so the half-open day window
                    # keeps exactly row 0.
                    tdSql.query(
                        f"SELECT v FROM {qualified} "
                        f"WHERE ts >= '{day1}' AND ts < '{day2}' ORDER BY ts")
                    tdSql.checkRows(1)
                    tdSql.checkData(0, 0, ROWS[0].v)

                    # Exact epoch bounds: the upper bound is the row's own
                    # value, so a bound converted at a coarser precision
                    # would drop the last row.
                    last_ns = expected_value(ROWS[1], entry["pk"],
                                             timezone_name,
                                             entry["transport"], "ns")
                    first_ns = expected_value(ROWS[0], entry["pk"],
                                              timezone_name,
                                              entry["transport"], "ns")
                    tdSql.query(
                        f"SELECT v FROM {qualified} "
                        f"WHERE ts >= {first_ns} AND ts <= {last_ns} ORDER BY ts")
                    tdSql.checkRows(2)
                    tdSql.checkData(0, 0, ROWS[0].v)
                    tdSql.checkData(1, 0, ROWS[1].v)
        finally:
            self._use_timezone(_DEFAULT_TZ)
        print("vtable time range predicates ............ [ passed ]")

    # ------------------------------------------------------------------
    # FQVT-TTP-007: virtual-super-table (USING) path
    # ------------------------------------------------------------------
    def do_vstable_path(self):
        self._use_timezone(_DEFAULT_TZ)
        ts_labels = {
            MYSQL.key: "datetime6",
            PG.key: "timestamp6",
            SQLSERVER.key: "datetimeoffset_7",
            ORACLE.key: "timestamp9",
        }
        # The microsecond stable only proves the column mapping; the nanosecond
        # stable also proves the source precision survives, because a
        # microsecond-truncated value would be visible there.
        for dst in ("us", "ns"):
            database = _LOCAL_DBS[dst]
            stable = "%s_%s" % (_VSTABLE, dst)
            tdSql.execute(f"DROP STABLE IF EXISTS {database}.{stable}")
            tdSql.execute(
                f"CREATE STABLE {database}.{stable} "
                f"(ts timestamp, c timestamp, v bigint) "
                f"TAGS (site nchar(16)) VIRTUAL 1")
            try:
                for backend in BACKENDS:
                    if backend.key == INFLUX.key:
                        continue
                    ts_type = _find_type(backend, ts_labels[backend.key])
                    table, pk_type, cols = self._mixed_tables[backend.key][0]
                    transport = backend.transport_rank([pk_type] + list(cols))
                    ctb = "ctb_%s_%s" % (backend.key, dst)
                    tdSql.execute(f"DROP VTABLE IF EXISTS {database}.{ctb}")
                    tdSql.execute(
                        f"CREATE VTABLE {database}.{ctb} "
                        f"(c FROM {backend.col_ref(table, ts_type.col)}, "
                        f"v FROM {backend.col_ref(table, 'v')}) "
                        f"USING {database}.{stable} TAGS ('{backend.key}')")

                    tdSql.query(
                        f"SELECT CAST(c AS BIGINT), v FROM {database}.{ctb} ORDER BY ts")
                    tdSql.checkRows(len(ROWS))
                    for row_index, row in enumerate(ROWS):
                        tdSql.checkData(
                            row_index, 0,
                            expected_value(row, ts_type, _DEFAULT_TZ, transport, dst))
                        tdSql.checkData(row_index, 1, row.v)

                    # FQVT-TTP-007b: the same rows through the virtual STABLE
                    # (fan-out).  The time axis must come from the child's
                    # source timestamp, the secondary TIMESTAMP column must
                    # keep its own slot, and the source precision must survive.
                    # Regression: the fan-out mapped every TIMESTAMP-typed slot
                    # to the time slot (ts took the secondary column, which
                    # then came back NULL) and the secondary column was decoded
                    # at microsecond precision.
                    tdSql.query(
                        f"SELECT CAST(ts AS BIGINT), CAST(c AS BIGINT), v "
                        f"FROM {database}.{stable} "
                        f"WHERE site = '{backend.key}' ORDER BY ts")
                    tdSql.checkRows(len(ROWS))
                    for row_index, row in enumerate(ROWS):
                        tdSql.checkData(
                            row_index, 0,
                            expected_value(row, pk_type, _DEFAULT_TZ, transport, dst))
                        tdSql.checkData(
                            row_index, 1,
                            expected_value(row, ts_type, _DEFAULT_TZ, transport, dst))
                        tdSql.checkData(row_index, 2, row.v)

                    tdSql.execute(f"DROP VTABLE {database}.{ctb}")
            finally:
                tdSql.execute(f"DROP STABLE IF EXISTS {database}.{stable}")
        print("virtual super table path ................ [ passed ]")

    # ------------------------------------------------------------------
    # FQVT-TTP-008: time functions on the virtual time axis
    # ------------------------------------------------------------------
    def do_time_functions(self):
        try:
            for timezone_name in ("Asia/Shanghai", "UTC"):
                self._use_timezone(timezone_name)
                for entry in self._mixed_vts:
                    if entry["db"] != "ns":
                        continue
                    qualified = f"{_LOCAL_DBS['ns']}.{entry['vt']}"
                    first_ns = expected_value(ROWS[0], entry["pk"],
                                              timezone_name,
                                              entry["transport"], "ns")
                    last_ns = expected_value(ROWS[1], entry["pk"],
                                             timezone_name,
                                             entry["transport"], "ns")

                    # Second truncation must work on the virtual timeline and
                    # keep the nanosecond unit of the destination database.
                    tdSql.query(
                        f"SELECT CAST(TIMETRUNCATE(ts, 1s) AS BIGINT) "
                        f"FROM {qualified} ORDER BY ts")
                    tdSql.checkRows(len(ROWS))
                    tdSql.checkData(0, 0, first_ns // 10 ** 9 * 10 ** 9)
                    tdSql.checkData(1, 0, last_ns // 10 ** 9 * 10 ** 9)

                    # Aggregates over the virtual timeline keep the exact
                    # transport value of the boundary rows.
                    tdSql.query(
                        f"SELECT CAST(MIN(ts) AS BIGINT), "
                        f"CAST(MAX(ts) AS BIGINT) FROM {qualified}")
                    tdSql.checkRows(1)
                    tdSql.checkData(0, 0, first_ns)
                    tdSql.checkData(0, 1, last_ns)
        finally:
            self._use_timezone(_DEFAULT_TZ)
        print("vtable time functions ................... [ passed ]")

    # ------------------------------------------------------------------
    # FQVT-TTP-001b: InfluxDB time axis
    # ------------------------------------------------------------------
    def do_influx_axis(self):
        entry = next(e for e in self._axis_vts if e["type"] is None)
        backend = entry["backend"]
        vtable_sql = (f"SELECT CAST(ts AS BIGINT), v "
                      f"FROM {_LOCAL_DBS['ns']}.{entry['vt']} ORDER BY ts")
        direct_sql = (f"SELECT CAST(time AS BIGINT), v "
                      f"FROM {backend.direct_table_ref(entry['table'])} "
                      f"ORDER BY time")

        # Wait for the measurement to become readable through the connector.
        deadline = time.monotonic() + _INFLUX_PERSIST_TIMEOUT_S
        while True:
            tdSql.query(direct_sql)
            if tdSql.queryRows > 0 or time.monotonic() >= deadline:
                break
            time.sleep(5)

        if tdSql.queryRows == 0:
            # Distinguish "our fixture is missing" from "the server holds the
            # rows but its FlightSQL view has none": InfluxDB 3 answers its own
            # HTTP SQL API from the WAL, while FlightSQL reads only Parquet,
            # so an unpersisted WAL leaves the connector blind.
            try:
                server_rows = backend.count_via_http(entry["table"])
            except Exception as exc:  # noqa: BLE001 - diagnostic only
                server_rows = -1
                tdLog.info(f"InfluxDB HTTP probe failed: {exc}")
            if server_rows > 0:
                pytest.skip(
                    "InfluxDB holds %d row(s) for %s.%s but its FlightSQL view "
                    "is empty (WAL not persisted to Parquet); the connector "
                    "cannot read what the server has, so the InfluxDB leg is "
                    "skipped instead of failing the precision matrix"
                    % (server_rows, backend.database, entry["table"]))
            tdSql.checkRows(len(ROWS))  # fixture missing: a real failure

        tdSql.query(direct_sql)
        tdSql.checkRows(len(ROWS))
        for index, row in enumerate(ROWS):
            tdSql.checkData(index, 0,
                            expected_value(row, INFLUX_TYPES[0], _DEFAULT_TZ,
                                           "ns", "ns"))
            tdSql.checkData(index, 1, row.v)

        tdSql.query(vtable_sql)
        tdSql.checkRows(len(ROWS))
        for index, row in enumerate(ROWS):
            ns = expected_value(row, INFLUX_TYPES[0], _DEFAULT_TZ, "ns", "ns")
            tdSql.checkData(index, 0, ns)
            tdSql.checkData(index, 1, row.v)
        print("vtable InfluxDB time axis ............... [ passed ]")

    # ------------------------------------------------------------------
    # pytest entry points
    # ------------------------------------------------------------------
    def test_vtable_time_axis_types(self):
        """Every external time type as the virtual table time axis

        1. Read each axis through a virtual table in a local database whose
           precision matches the column's declared precision.
        2. Check the epoch of both fixture rows exactly.
        3. Cover the ms -> us -> ns upscale of representative axes.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-05 wpan Add the virtual-table precision/timezone matrix

        """
        self.do_axis_matrix()

    def test_vtable_influx_time_axis(self):
        """InfluxDB measurement as the virtual time axis

        1. Wait for the measurement to become readable through the connector.
        2. Verify the direct external read and the virtual table agree on the
           exact UTC nanosecond epoch.
        3. Skip with a precise diagnostic only when the InfluxDB server holds
           the rows but its FlightSQL view is empty (WAL not persisted).

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-05 wpan Add the virtual-table precision/timezone matrix

        """
        self.do_influx_axis()

    def test_vtable_non_key_time_columns(self):
        """Every external time type as a non-key virtual column

        1. Read all non-key time columns through ms / us / ns virtual tables.
        2. Check every cell against the transport-then-destination scaling.
        3. Regression: an Oracle TIMESTAMP(7..9) non-key column must keep its
           sub-microsecond digits in a nanosecond virtual table.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-05 wpan Add the virtual-table precision/timezone matrix

        """
        self.do_non_key_matrix()

    def test_vtable_matches_direct_read(self):
        """A virtual table returns what the external table returns

        1. Read every non-key time column directly from the external source.
        2. Read the same columns through a nanosecond virtual table.
        3. Require the virtual value to equal the direct value scaled to ns.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-05 wpan Add the virtual-table precision/timezone matrix

        """
        self.do_direct_parity()

    def test_vtable_precision_rejected(self):
        """A virtual table finer than the source time axis is rejected

        1. For every source and every time-axis type, try to create the
           virtual table in every coarser local precision.
        2. Require 0x268f from each attempt.
        3. Require the virtual-super-table path to enforce the same contract.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-05 wpan Add the virtual-table precision/timezone matrix

        """
        self.do_precision_negative()

    def test_vtable_timezone(self):
        """Timezone semantics of aware and naive virtual columns

        1. Sweep the client timezone over CST / UTC / US Eastern.
        2. Check every non-key column against the timezone-aware expectation.
        3. Require tz-aware instants to stay fixed and naive wall clocks to
           follow the client timezone.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-05 wpan Add the virtual-table precision/timezone matrix

        """
        self.do_timezone_matrix()

    def test_vtable_time_range(self):
        """Time-range predicates on the virtual time axis

        1. Calendar-day bounds select the same row across client timezones.
        2. Epoch bounds written at the column's own instant keep the last row.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-05 wpan Add the virtual-table precision/timezone matrix

        """
        self.do_time_range()

    def test_vtable_time_functions(self):
        """Time functions evaluated on the virtual time axis

        1. TIMETRUNCATE keeps the destination nanosecond unit.
        2. MIN/MAX return the exact transport value of the boundary rows.
        3. Both run under CST and UTC session timezones.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-05 wpan Add the virtual-table precision/timezone matrix

        """
        self.do_time_functions()

    def test_vtable_super_table_path(self):
        """Virtual-super-table (USING) path time type coverage

        1. Bind a non-key time column into a virtual child table.
        2. Check the value at the stable's declared precision.

        Catalog:
            - VirtualTable

        Since: v3.5.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-05 wpan Add the virtual-table precision/timezone matrix

        """
        self.do_vstable_path()
