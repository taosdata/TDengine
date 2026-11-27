# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
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

# -*- coding: utf-8 -*-
"""
Batch meta txn: ADD SERIES / REMOVE SERIES inside a transaction.

Covers FS 《元数据 DDL 支持事务二期-支持更多操作类型 FS》 §4.11.3 actions #22/#23 and
the SERIES paragraph of §4.11.4. These are the two ALTER actions the sibling
`test_meta_batch_txn_op_matrix.py` cannot cover: SERIES only exists for InfluxDB
external sources, so exercising it needs a live InfluxDB instance, which the
op_matrix file (a plain `ci` case with no external dependency) must not require.

A SERIES is a named binding from an alias to one InfluxDB series — the set of
points sharing a fully-specified tag set within a measurement. Unlike a column
ref, ADD/REMOVE SERIES does not change the column schema at all: it only
rewrites the entry's `series` array and bumps `colRef.version`. That is exactly
why it needs its own coverage — a rollback that restored only the column schema
would look correct to every other case in the matrix.

  x1  ADD SERIES: in-txn visible, invisible to other sessions, ROLLBACK reverts
  x2  ADD SERIES + COMMIT publishes to the whole cluster, and the bound column
      reads through to InfluxDB afterwards
  x3  REMOVE SERIES of a series that predates the txn: ROLLBACK restores it,
      COMMIT publishes its removal
  x4  Repeated ADD/REMOVE of the same alias in one txn: `txnOrigVer` must keep
      pointing at the pre-txn version rather than an intermediate one, so both
      the out-of-txn read and the rollback still yield the pre-txn series set
  x5  SERIES mixed with a column ref repoint in one txn (FS §7.4 pattern):
      ROLLBACK restores the series set and the ref together
"""

import os
import re
import sys

from new_test_framework.utils import tdLog, tdSql, tdCom

_CASES_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
# ExtSrcEnv (external-source lifecycle) and the DDL helpers that wrap it live in
# two different suites; both directories have to be importable.
for _p in (os.path.join(_CASES_DIR, "09-DataQuerying", "19-FederatedQuery"),
           os.path.join(_CASES_DIR, "05-VirtualTables", "01-DDL", "03-ExtSource")):
    if _p not in sys.path:
        sys.path.insert(0, _p)
from federated_query_common import ExtSrcEnv  # noqa: E402
from ext_source_helpers import (  # noqa: E402
    create_ext_source, create_remote_db, create_influx_measurement)

_LOCAL_DB = "txn_series_db"
_INF_DB = "txn_series_inf"
_INF_SRC = "txn_series_inf_src"
_MEAS = "txn_m"

# InfluxDB line protocol is nanosecond precision, so the local db is created
# with PRECISION 'ns' to match.
_T0 = 1700000000000000000


def _write_influx_data():
    """Three tag sets under one measurement, with distinct row counts so a
    query can tell which series a column is actually pinned to."""
    create_remote_db("influxdb", _INF_DB)
    lines = []
    for i in range(3):
        lines.append(f"{_MEAS},device=d1 value={1.0 + i},extra={10 + i}i "
                     f"{_T0 + i * 1000000000}")
    for i in range(2):
        lines.append(f"{_MEAS},device=d2 value={5.0 + i},extra={50 + i}i "
                     f"{_T0 + i * 1000000000}")
    lines.append(f"{_MEAS},device=d3 value=9.0,extra=90i {_T0}")
    create_influx_measurement(_INF_DB, lines)
    # InfluxDB 3.x only serves rows to FlightSQL once they are snapshotted out
    # of the WAL; writing one extra point forces that snapshot.
    create_influx_measurement(
        _INF_DB, ["_snapshot_trigger_ value=1i 1000000000000000000"])


class TestBatchMetaTxnSeries:

    @classmethod
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        # Only InfluxDB is needed here — avoid ExtSrcEnv.ensure_env(), which also
        # requires MySQL/PG to be reachable.
        ExtSrcEnv.start_influx_instance(ExtSrcEnv.INFLUX_VERSIONS[0])
        ExtSrcEnv.ensure_qnode()
        _write_influx_data()
        create_ext_source(_INF_SRC, "influxdb", _INF_DB)
        tdSql.execute(f"drop database if exists {_LOCAL_DB}")
        tdSql.execute(f"create database {_LOCAL_DB} precision 'ns' keep 36500")
        tdSql.execute(f"use {_LOCAL_DB}")

    @classmethod
    def teardown_class(cls):
        tdSql.execute_ignore_error("ROLLBACK")
        tdSql.execute(f"drop database if exists {_LOCAL_DB}")
        tdSql.execute(f"drop external source if exists {_INF_SRC}")
        # DROP EXTERNAL SOURCE only removes the local definition.  The
        # InfluxDB bucket outlives it and keeps occupying one of the five
        # database slots InfluxDB 3 Core allows per instance, so a full run
        # that visits several ext-source suites eventually makes every later
        # influx_create_db() fail with HTTP 422.
        ExtSrcEnv.influx_drop_db(_INF_DB)
        tdLog.debug("%s teardown done" % __file__)

    # ---------------------------------------------------------------- helpers

    def _reset(self):
        tdSql.execute_ignore_error("ROLLBACK")
        tdSql.execute(f"use {_LOCAL_DB}")
        for show, drop in [("show vtables", "drop table"),
                           ("show stables", "drop stable"),
                           ("show tables", "drop table")]:
            tdSql.query(show)
            for i in range(tdSql.queryRows):
                tdSql.execute_ignore_error(f"{drop} if exists {tdSql.queryResult[i][0]}")

    def _other(self):
        """A second session, used to observe out-of-txn visibility."""
        s = tdCom.newTdSql()
        s.execute(f"use {_LOCAL_DB}")
        return s

    def _series_clause(self, alias, device):
        return (f"SERIES {alias} AS {_INF_SRC}.{_INF_DB}.{_MEAS} "
                f"(device='{device}')")

    def _create_sql(self, table, session=None):
        """The CREATE statement `SHOW CREATE VTABLE` renders back out. It is the
        only place the series set surfaces in SQL — DESCRIBE shows the per-column
        ref, but a series carrying no column would not appear there at all."""
        s = session or tdSql
        s.query(f"show create vtable {table}")
        assert s.queryRows == 1, f"show create vtable {table} returned {s.queryRows} rows"
        for c in range(s.queryCols):
            v = s.getData(0, c)
            if isinstance(v, str) and "CREATE" in v.upper():
                return v
        raise AssertionError(f"no CREATE statement for {table}")

    def _aliases(self, table, session=None):
        """The set of series aliases currently bound to `table`.

        Parsed off SHOW CREATE rather than assembled from a catalog query: the
        rendered clause is what a user would see, so an alias that survives in
        the entry but stops being rendered is a bug this should catch.
        """
        sql = self._create_sql(table, session)
        # SERIES clauses render as: SERIES <alias> AS <src>.<db>.<meas> (tags).
        # Matched with a regex rather than by splitting on spaces so that extra
        # whitespace or a line break around the alias cannot silently yield an
        # incomplete set (which would show up as a confusing assertion failure).
        return set(re.findall(r"\bSERIES\s+`?([A-Za-z_]\w*)`?\s+AS\b", sql, re.IGNORECASE))

    def _cols(self, table, session=None):
        s = session or tdSql
        s.query(f"describe {table}")
        return {s.queryResult[i][0] for i in range(s.queryRows)}

    def _count(self, sql, expected):
        tdSql.query(sql)
        tdSql.checkData(0, 0, expected)

    def _setup_vt(self, name="vts", alias="s1", device="d1"):
        """A virtual normal table with one series-bound column."""
        tdSql.execute(
            f"create vtable {name} (ts timestamp, value double from {alias}.value) "
            f"{self._series_clause(alias, device)}")

    # =========================================================================
    # x1: ADD SERIES — in-txn visible, isolated, reverted by ROLLBACK
    # =========================================================================

    def x1_add_series_rollback(self):
        tdLog.info("======== x1_add_series_rollback")
        self._reset()
        self._setup_vt()
        s2 = self._other()
        assert self._aliases("vts") == {"s1"}, self._aliases("vts")

        tdSql.execute("BEGIN")
        tdSql.execute(f"alter vtable vts add {self._series_clause('s2', 'd2')}")
        assert self._aliases("vts") == {"s1", "s2"}, self._aliases("vts")
        # The other session must not see the new binding before COMMIT.
        assert self._aliases("vts", session=s2) == {"s1"}, \
            self._aliases("vts", session=s2)

        tdSql.execute("ROLLBACK")
        assert self._aliases("vts") == {"s1"}, self._aliases("vts")
        assert self._aliases("vts", session=s2) == {"s1"}
        # The pre-existing binding still resolves after the rollback.
        self._count("select count(*) from vts", 3)

        s2.close()
        tdLog.info("x1 PASSED: ADD SERIES isolation + rollback")

    # =========================================================================
    # x2: ADD SERIES — COMMIT publishes, and the new series is queryable
    # =========================================================================

    def x2_add_series_commit(self):
        tdLog.info("======== x2_add_series_commit")
        self._reset()
        self._setup_vt()
        s2 = self._other()

        tdSql.execute("BEGIN")
        tdSql.execute(f"alter vtable vts add {self._series_clause('s2', 'd2')}")
        # Bind a new column to the series added in this same txn, so the txn
        # covers both halves of the feature: the series and a column using it.
        tdSql.execute("alter vtable vts add column v2 double from s2.value")
        tdSql.execute("COMMIT")

        assert self._aliases("vts") == {"s1", "s2"}, self._aliases("vts")
        assert self._aliases("vts", session=s2) == {"s1", "s2"}, \
            self._aliases("vts", session=s2)
        assert "v2" in self._cols("vts", session=s2), self._cols("vts", session=s2)
        # d1 has 3 rows and d2 has 2: each column reads its own pinned series.
        self._count("select count(value) from vts", 3)
        self._count("select count(v2) from vts", 2)

        s2.close()
        tdLog.info("x2 PASSED: ADD SERIES commit + bound column queryable")

    # =========================================================================
    # x3: REMOVE SERIES of a pre-txn series
    #     ROLLBACK must bring back the series array of the original entry, not
    #     merely leave the column schema alone (the schema never changed).
    # =========================================================================

    def x3_remove_series(self):
        tdLog.info("======== x3_remove_series")
        self._reset()
        # Two series, only s1 carrying a column: s2 can then be removed without
        # first detaching a column from it.
        tdSql.execute(
            f"create vtable vts (ts timestamp, value double from s1.value) "
            f"{self._series_clause('s1', 'd1')} "
            f"{self._series_clause('s2', 'd2')}")
        s2 = self._other()
        assert self._aliases("vts") == {"s1", "s2"}, self._aliases("vts")

        tdSql.execute("BEGIN")
        tdSql.execute("alter vtable vts remove series s2")
        assert self._aliases("vts") == {"s1"}, self._aliases("vts")
        assert self._aliases("vts", session=s2) == {"s1", "s2"}, \
            self._aliases("vts", session=s2)

        tdSql.execute("ROLLBACK")
        assert self._aliases("vts") == {"s1", "s2"}, self._aliases("vts")

        tdSql.execute("BEGIN")
        tdSql.execute("alter vtable vts remove series s2")
        tdSql.execute("COMMIT")
        assert self._aliases("vts") == {"s1"}, self._aliases("vts")
        assert self._aliases("vts", session=s2) == {"s1"}, \
            self._aliases("vts", session=s2)
        # s1 is untouched by s2's removal.
        self._count("select count(*) from vts", 3)

        s2.close()
        tdLog.info("x3 PASSED: REMOVE SERIES isolation/rollback/commit")

    # =========================================================================
    # x4: repeated ADD/REMOVE of the same alias in one txn
    #     Each ALTER writes a new entry version. `txnOrigVer` must stay pinned to
    #     the pre-txn version; if an intermediate version overwrote it, the
    #     rollback would restore a mid-transaction series set instead.
    # =========================================================================

    def x4_repeated_add_remove_same_txn(self):
        tdLog.info("======== x4_repeated_add_remove_same_txn")
        self._reset()
        self._setup_vt()          # pre-txn set: {s1}
        s2 = self._other()

        tdSql.execute("BEGIN")
        tdSql.execute(f"alter vtable vts add {self._series_clause('s2', 'd2')}")
        tdSql.execute(f"alter vtable vts add {self._series_clause('s3', 'd3')}")
        tdSql.execute("alter vtable vts remove series s2")
        tdSql.execute(f"alter vtable vts add {self._series_clause('s2', 'd2')}")
        tdSql.execute("alter vtable vts remove series s3")
        assert self._aliases("vts") == {"s1", "s2"}, self._aliases("vts")
        # Throughout the churn the other session keeps seeing the pre-txn set.
        assert self._aliases("vts", session=s2) == {"s1"}, \
            self._aliases("vts", session=s2)

        tdSql.execute("ROLLBACK")
        assert self._aliases("vts") == {"s1"}, self._aliases("vts")
        self._count("select count(*) from vts", 3)

        s2.close()
        tdLog.info("x4 PASSED: repeated ADD/REMOVE keeps txnOrigVer at pre-txn version")

    # =========================================================================
    # x5: SERIES together with a column-ref repoint in one txn (FS §7.4)
    # =========================================================================

    def x5_series_with_colref(self):
        tdLog.info("======== x5_series_with_colref")
        self._reset()
        self._setup_vt()          # value -> s1.value, series {s1}
        s2 = self._other()
        self._count("select count(value) from vts", 3)

        tdSql.execute("BEGIN")
        tdSql.execute(f"alter vtable vts add {self._series_clause('s2', 'd2')}")
        # Repoint the existing column from s1 to the series added above.
        tdSql.execute("alter vtable vts alter column value set s2.value")
        tdSql.execute("ROLLBACK")

        assert self._aliases("vts") == {"s1"}, self._aliases("vts")
        # The ref went back to s1 (3 rows), not s2 (2 rows).
        self._count("select count(value) from vts", 3)

        tdSql.execute("BEGIN")
        tdSql.execute(f"alter vtable vts add {self._series_clause('s2', 'd2')}")
        tdSql.execute("alter vtable vts alter column value set s2.value")
        tdSql.execute("COMMIT")

        assert self._aliases("vts") == {"s1", "s2"}, self._aliases("vts")
        assert self._aliases("vts", session=s2) == {"s1", "s2"}
        self._count("select count(value) from vts", 2)

        s2.close()
        tdLog.info("x5 PASSED: SERIES + column ref rollback/commit together")

    # =========================================================================
    # Test entry
    # =========================================================================

    def test_meta_batch_txn_series(self):
        """Batch meta txn: ADD/REMOVE SERIES (actions #22/#23) inside a transaction.

        SERIES is an InfluxDB-only binding from an alias to one fully-specified
        tag set. Its ALTER actions change only the entry's `series` array and
        `colRef.version`, never the column schema, so they need coverage
        independent of the column-ref cases in the op_matrix file.

        x1. ADD SERIES is visible in-txn, invisible to other sessions, undone by
            ROLLBACK
        x2. ADD SERIES + a column bound to it COMMIT together and the column then
            reads through to the pinned InfluxDB series
        x3. REMOVE SERIES of a pre-txn series: ROLLBACK restores the series array
            even though no column schema ever changed
        x4. Repeated ADD/REMOVE of one alias in a single txn keeps `txnOrigVer` at
            the pre-txn version, so the rollback restores the pre-txn set rather
            than an intermediate one
        x5. SERIES plus a column-ref repoint in one txn revert and commit together
            (FS §7.4)

        Since: v3.4.3.0

        Labels: common
        """
        self.x1_add_series_rollback()
        self.x2_add_series_commit()
        self.x3_remove_series()
        self.x4_repeated_add_remove_same_txn()
        self.x5_series_with_colref()
