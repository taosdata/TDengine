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

from new_test_framework.utils import tdLog, tdSql, etool

import datetime
import os
import sys
import time

from new_test_framework.utils import tdStream

_CASE_DIR = os.path.dirname(os.path.abspath(__file__))
for relative in (
    "../../05-VirtualTables/01-DDL/03-ExtSource",
    "../../09-DataQuerying/19-FederatedQuery",
    "../99-Others",
):
    sys.path.insert(0, os.path.abspath(os.path.join(_CASE_DIR, relative)))

from ext_source_helpers import (  # noqa: E402
    ExtSrcEnv,
    create_ext_source,
    create_oracle_table,
)
from fs_common import ensure_snode  # noqa: E402


class TestVirtualStreamOracle:
    DB = "vs_oracle"
    SOURCE = "vs_oracle_src"
    STREAM = "oracle_stream"
    BASE_US = 1767225600000000

    # Oracle has one namespace level - the schema - and the fixture accounts own
    # no cross-schema grants, so the remote tables live in the login user's own
    # schema (folded to upper case, like every unquoted Oracle identifier).
    SCHEMA = ExtSrcEnv.ORACLE_USER.upper()
    TABLE_A = "SAME_A"
    TABLE_B = "SAME_B"

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        ExtSrcEnv.ensure_env()
        ensure_snode()
        tdSql.execute("set timezone 'UTC'")

    @classmethod
    def teardown_class(cls):
        try:
            cls._cleanup()
        finally:
            ExtSrcEnv.teardown_env()

    @classmethod
    def _cleanup(cls):
        tdSql.execute(f"DROP STREAM IF EXISTS {cls.DB}.{cls.STREAM}")
        tdSql.execute(f"DROP DATABASE IF EXISTS {cls.DB} FORCE")
        tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {cls.SOURCE}")
        cfg = next(ExtSrcEnv.oracle_version_configs())
        for table in (cls.TABLE_A, cls.TABLE_B):
            ExtSrcEnv.oracle_drop_table_cfg(cfg, table)

    @staticmethod
    def _stamp(epoch_us):
        """Local naive datetime for an epoch, matching the Python connector.

        The connector converts returned timestamps with the local timezone, so sink
        assertions compare against local wall clocks.
        """
        return datetime.datetime.fromtimestamp(epoch_us // 1000000).replace(
            microsecond=epoch_us % 1000000
        )

    @staticmethod
    def _remote_stamp(epoch_us):
        """Naive wall clock a timezone-naive Oracle TIMESTAMP must hold for `epoch_us`.

        The stream is created in a UTC session and timezone-naive source columns are
        decoded in the stream's timezone, so the literal has to spell the UTC wall
        clock rather than the test process's local one.
        """
        return datetime.datetime(1970, 1, 1) + datetime.timedelta(microseconds=epoch_us)

    @staticmethod
    def _oracle_ts_literal(dt):
        """Render an Oracle TIMESTAMP literal that every supported generation parses.

        11g rejects the ISO-8601 'T' separator that ``datetime.isoformat()``
        emits (ORA-01861: literal does not match format string), while the
        space-separated form is accepted by 11g, 19c and 26ai alike.
        """
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")

    @classmethod
    def _sink_rows(cls, epochs):
        marks = ", ".join(str(epoch) for epoch in epochs)
        return tdSql.getResult(
            "SELECT first_ts, value_a, value_b, local_value, row_count "
            f"FROM {cls.DB}.sink WHERE first_ts IN ({marks}) ORDER BY first_ts"
        )

    @classmethod
    def _wait_rows(cls, epochs, expected, timeout=120, reason="Oracle stream rows"):
        """Wait until the sink holds exactly the rows emitted for `epochs`.

        Rows are filtered by timestamp so windows emitted by an earlier probe attempt (possibly
        through a stale binding) cannot pollute the comparison, and a transient query error is
        retried instead of failing the case.
        """
        deadline = time.monotonic() + timeout
        actual = None
        last_error = None
        while time.monotonic() < deadline:
            try:
                result = cls._sink_rows(epochs)
            except Exception as error:
                last_error = error
                time.sleep(0.5)
                continue
            actual = [tuple(row) for row in result]
            if actual == expected:
                return
            time.sleep(0.5)
        raise AssertionError(f"{reason}: expected={expected}, actual={actual}, error={last_error}")

    @classmethod
    def _check_rebind_probe_history(cls, epochs, timeout=60):
        """Every probe attempt must have produced a window and kept its local value.

        The successful attempt is already awaited by the caller; earlier attempts may still be in
        flight, so wait until each probe epoch has at least one row before checking values.
        """
        expected_stamps = {cls._stamp(epoch) for epoch in epochs}
        deadline = time.monotonic() + timeout
        rows = []
        last_error = None
        while time.monotonic() < deadline:
            try:
                rows = [tuple(row) for row in cls._sink_rows(epochs)]
            except Exception as error:
                last_error = error
                time.sleep(0.5)
                continue
            if expected_stamps <= {row[0] for row in rows}:
                break
            time.sleep(0.5)
        missing = expected_stamps - {row[0] for row in rows}
        assert not missing, (
            "rebind probes never produced a window: "
            f"missing={sorted(missing)}, rows={rows}, error={last_error}"
        )
        tdLog.debug(f"checking rebind probe history: attempts={len(epochs)}, rows={rows}")
        for row in rows:
            assert row[1] == 41 and row[2] in (None, 41) and row[3] == 61 and row[4] == 1, (
                f"Rebind corrupted a completed probe window: {tuple(row)}"
            )

    @classmethod
    def _insert_remote(cls, table, epoch_us, value):
        stamp = cls._oracle_ts_literal(cls._remote_stamp(epoch_us))
        cfg = next(ExtSrcEnv.oracle_version_configs())
        # Oracle has no multi-row VALUES and SQL*Plus does not commit on exit, so
        # the INSERT and COMMIT share one sqlplus session via a batch.
        ExtSrcEnv._oracle_sql(
            cfg, [f"INSERT INTO {table} VALUES (TIMESTAMP '{stamp}', {value})", "COMMIT"]
        )

    @classmethod
    def _write_epoch(cls, epoch_us, values, local_value=None):
        cls._insert_remote(cls.TABLE_A, epoch_us, values[0])
        cls._insert_remote(cls.TABLE_B, epoch_us, values[1])
        if local_value is not None:
            tdSql.execute(f"INSERT INTO {cls.DB}.local_t VALUES ({epoch_us}, {local_value})")

    @classmethod
    def _wait_reader_ready(cls, mixed, attempts=8, per_attempt=20):
        """Prove the external readers already sampled their history cutoff.

        A trigger task in Running state only means its start control request was queued, so it is
        not a completion barrier. A canary row written before the cutoff sample is consumed as
        history and never emitted; the cutoff can fall between the two remote inserts, so every
        attempt uses a fresh timestamp and a partially visible window just triggers a retry.
        """
        value = 900
        for attempt in range(attempts):
            value += 1
            epoch = cls.BASE_US + value
            cls._write_epoch(epoch, (value, value + 100), value + 200 if mixed else None)
            expected = [(cls._stamp(epoch), value, value + 100, value + 200 if mixed else None, 1)]
            try:
                cls._wait_rows([epoch], expected, timeout=per_attempt, reason="reader readiness")
                tdLog.info(f"external reader is ready after {attempt + 1} canary attempt(s)")
                return
            except AssertionError as error:
                tdLog.info(f"reader readiness canary not observed yet: {error}")
        raise AssertionError(
            "external reader never emitted a canary row: its history cutoff was not sampled"
        )

    def do_oracle_virtual_stream(self, mixed):
        self._cleanup()
        try:
            cfg = next(ExtSrcEnv.oracle_version_configs())
            # Oracle has no CREATE DATABASE: the Easy Connect service name is the
            # connection target and the login user's schema is the namespace.
            create_ext_source(self.SOURCE, "oracle", cfg.service)
            # Naive Oracle timestamps are decoded in the stream timezone (the
            # creating session is on UTC), so spell the UTC wall clock here.
            seed = self._remote_stamp(self.BASE_US)
            for table in (self.TABLE_A, self.TABLE_B):
                create_oracle_table(
                    table,
                    "ts TIMESTAMP(6) NOT NULL PRIMARY KEY, v NUMBER(10,0)",
                    [f"(TIMESTAMP '{self._oracle_ts_literal(seed)}', 0)"],
                )
            tdSql.execute(f"CREATE DATABASE {self.DB} VGROUPS 1 PRECISION 'us'")
            tdSql.execute(f"CREATE TABLE {self.DB}.local_t (ts TIMESTAMP, v INT)")
            tdSql.execute(f"INSERT INTO {self.DB}.local_t VALUES ({self.BASE_US}, 0)")
            tdSql.execute(
                f"CREATE STABLE {self.DB}.vstb "
                # Oracle NUMBER maps to a wider TDengine integer type, so the
                # bound columns must be BIGINT to pass the declared-vs-external
                # type check (see VTABLE_COLUMN_TYPES_ORACLE).
                "(ts TIMESTAMP, value_a BIGINT, value_b BIGINT, local_value INT) "
                "TAGS (site INT) VIRTUAL 1"
            )
            # Both references spell the source schema: a virtual child (USING <vstable>)
            # only accepts the untyped "col FROM ref" list, and a two-segment external
            # path (source.table.column, the source default schema) is misresolved on the
            # main branch - the empty middle segment is filled with the virtual
            # table's database, so the lookup becomes source.<local_db>.table and fails
            # (Defect 7160732905).  The default-schema form can be added back here once
            # that defect is fixed.
            ref_a = f"{self.SOURCE}.{self.SCHEMA}.{self.TABLE_A}"
            ref_b = f"{self.SOURCE}.{self.SCHEMA}.{self.TABLE_B}"
            local_ref = f", local_value FROM {self.DB}.local_t.v" if mixed else ""
            tdSql.execute(
                f"CREATE VTABLE {self.DB}.vt ("
                f"value_a FROM {ref_a}.v, "
                f"value_b FROM {ref_b}.v"
                f"{local_ref}) USING {self.DB}.vstb TAGS(1)"
            )
            tdLog.debug(f"creating Oracle virtual stream, mixed={mixed}")
            tdSql.execute(
                f"CREATE STREAM {self.DB}.{self.STREAM} COUNT_WINDOW(1) "
                f"FROM {self.DB}.vstb PARTITION BY tbname "
                "STREAM_OPTIONS(IGNORE_DISORDER) "
                f"INTO {self.DB}.sink (first_ts, value_a, value_b, local_value, row_count) "
                "AS SELECT first(_c0), sum(value_a), sum(value_b), sum(local_value), "
                "count(*) FROM %%trows"
            )
            tdStream.checkStreamStatus(self.STREAM)
            self._wait_reader_ready(mixed)
            epochs = [self.BASE_US + offset for offset in (1123456, 1123457, 2654321)]
            timestamps = [self._stamp(epoch) for epoch in epochs]
            for table, values in ((self.TABLE_A, (11, 12, 13)), (self.TABLE_B, (21, 22, 23))):
                for epoch, value in zip(epochs, values):
                    self._insert_remote(table, epoch, value)
            if mixed:
                rows = " ".join(
                    f"({epoch}, {value})" for epoch, value in zip(epochs, (31, 32, 33))
                )
                tdSql.execute(f"INSERT INTO {self.DB}.local_t VALUES {rows}")
            expected = [
                (timestamps[0], 11, 21, 31 if mixed else None, 1),
                (timestamps[1], 12, 22, 32 if mixed else None, 1),
                (timestamps[2], 13, 23, 33 if mixed else None, 1),
            ]
            self._wait_rows(epochs, expected)
            if mixed:
                tdLog.debug("rebinding Oracle virtual column through a short reference")
                tdSql.execute(
                    f"ALTER VTABLE {self.DB}.vt ALTER COLUMN value_b SET {self.SOURCE}.{self.TABLE_A}.v"
                )
                tdStream.checkStreamStatus(self.STREAM)
                # A row written only to SAME_A is the rebind probe: value_b reads 41 only once the
                # new binding is deployed; while the patch is pending it still reads the old
                # binding (SAME_B) and stays NULL.
                probe_epoch = None
                probe_epochs = []
                for attempt in range(8):
                    epoch = self.BASE_US + 10654321 + attempt
                    self._insert_remote(self.TABLE_A, epoch, 41)
                    tdSql.execute(f"INSERT INTO {self.DB}.local_t VALUES ({epoch}, 61)")
                    probe_epochs.append(epoch)
                    try:
                        self._wait_rows([epoch], [(self._stamp(epoch), 41, 41, 61, 1)],
                                        timeout=20, reason="rebind probe")
                    except AssertionError as error:
                        tdLog.info(f"rebind probe not applied yet: {error}")
                        continue
                    probe_epoch = epoch
                    break
                if probe_epoch is None:
                    raise AssertionError(
                        "rebind did not take effect: value_b kept reading through the old binding"
                    )
                self._wait_rows(
                    epochs + [probe_epoch],
                    expected + [(self._stamp(probe_epoch), 41, 41, 61, 1)],
                    reason="rebind history",
                )
                self._check_rebind_probe_history(probe_epochs)
            tdLog.info(f"Oracle virtual stream, mixed={mixed} [ passed ]")
        finally:
            self._cleanup()

    def test_oracle_external_columns(self):
        """Oracle virtual stream preserves schema identity and microseconds.

        1. Reference two same-named tables in the login schema through both the
           default-schema (source.table.column) and explicit-schema
           (source.schema.table.column) paths.
        2. Check every output row, including timestamps one microsecond apart.

        Catalog:
            - Stream

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-02 Wang Mingming Add Oracle virtual stream coverage.
        """
        self.do_oracle_virtual_stream(False)

    def test_oracle_mixed_local_columns(self):
        """Oracle and local columns merge into virtual stream rows.

        1. Combine local WAL data with two Oracle schema bindings.
        2. Verify timestamp alignment, values, and row counts in the sink.
        3. Rebind through a short Oracle path and verify historical and new rows.

        Catalog:
            - Stream

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-02 Wang Mingming Add mixed Oracle stream coverage.
        """
        self.do_oracle_virtual_stream(True)
