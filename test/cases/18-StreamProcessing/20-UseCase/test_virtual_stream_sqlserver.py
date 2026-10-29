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
    create_remote_db,
    create_sqlserver_table,
)
from fs_common import ensure_snode  # noqa: E402


class TestVirtualStreamSqlserver:
    DB = "vs_sqlserver"
    REMOTE_DB = "vs_sqlserver_remote"
    SOURCE = "vs_sqlserver_src"
    STREAM = "sqlserver_stream"
    BASE_US = 1767225600000000

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
        ExtSrcEnv.sqlserver_drop_db(cls.REMOTE_DB)

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
        """Naive wall clock a timezone-naive SQL Server column must hold for `epoch_us`.

        The stream is created in a UTC session and timezone-naive source columns are
        decoded in the stream's timezone, so the literal has to spell the UTC wall
        clock rather than the test process's local one.
        """
        return datetime.datetime(1970, 1, 1) + datetime.timedelta(microseconds=epoch_us)

    @classmethod
    def _sink_rows(cls, epochs):
        marks = ", ".join(str(epoch) for epoch in epochs)
        return tdSql.getResult(
            "SELECT first_ts, value_a, value_b, local_value, row_count "
            f"FROM {cls.DB}.sink WHERE first_ts IN ({marks}) ORDER BY first_ts"
        )

    @classmethod
    def _wait_rows(cls, epochs, expected, timeout=120, reason="SQL Server stream rows"):
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
        flight, so wait until each probe epoch has at least one row before checking values. A
        probe whose remote and local rows became visible at different times can emit more than
        one row for the same epoch, so only presence per epoch is required.
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
    def _insert_remote(cls, schema, epoch_us, value):
        stamp = cls._remote_stamp(epoch_us).isoformat(timespec="microseconds")
        ExtSrcEnv.sqlserver_exec(
            cls.REMOTE_DB, [f"INSERT INTO {schema}.same_t VALUES ('{stamp}', {value})"]
        )

    @classmethod
    def _wait_stream_failed(cls, stream, needle, timeout=90):
        """Wait until `stream` reports a failed status whose message contains `needle`."""
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            try:
                tdSql.query(
                    "SELECT status, message FROM information_schema.ins_streams "
                    f"WHERE db_name='{cls.DB}' AND stream_name='{stream}'")
                if tdSql.queryRows == 1:
                    last = (str(tdSql.getData(0, 0)), str(tdSql.getData(0, 1)))
                    if last[0].lower() == "failed" and needle in last[1]:
                        return
            except Exception as error:
                last = ("query-error", str(error))
            time.sleep(1)
        raise AssertionError(f"stream {stream} did not fail with '{needle}': {last}")

    @classmethod
    def _write_epoch(cls, epoch_us, values, local_value=None):
        cls._insert_remote("dbo", epoch_us, values[0])
        cls._insert_remote("other", epoch_us, values[1])
        if local_value is not None:
            tdSql.execute(f"INSERT INTO {cls.DB}.local_t VALUES ({epoch_us}, {local_value})")

    @classmethod
    def _wait_reader_ready(cls, mixed, attempts=8, per_attempt=20):
        """Prove the external readers already sampled their history cutoff.

        A trigger task in Running state only means its start control request was queued
        (streamTriggerTask.c sets STREAM_STATUS_RUNNING right after the enqueue), so it is not a
        completion barrier. A probe row is: a row written before the cutoff sample is consumed as
        history and never emitted. The cutoff can fall between the dbo/other inserts, so every
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

    @classmethod
    def _dto_literal(cls, epoch_us, scale):
        """Render an explicit +00:00 datetimeoffset literal at `scale` digits.

        The offset is spelled out so the literal is an absolute instant, independent of
        the test process and taosd timezones.
        """
        stamp = cls._remote_stamp(epoch_us).strftime("%Y-%m-%dT%H:%M:%S.%f")
        if scale > 0:
            stamp = stamp[: 19 + 1 + scale]
        else:
            stamp = stamp[:19]
        return f"{stamp}+00:00"

    @classmethod
    def _insert_dto(cls, epoch_us, value, scale):
        ExtSrcEnv.sqlserver_exec(
            cls.REMOTE_DB, [f"INSERT INTO dbo.dto_t VALUES ('{cls._dto_literal(epoch_us, scale)}', {value})"]
        )

    @classmethod
    def _dto_sink_rows(cls, epochs):
        marks = ", ".join(str(epoch) for epoch in epochs)
        return tdSql.getResult(
            "SELECT first_ts, value_a, row_count "
            f"FROM {cls.DB}.sink WHERE first_ts IN ({marks}) ORDER BY first_ts"
        )

    @classmethod
    def _wait_dto_rows(cls, epochs, expected, timeout=120, reason="datetimeoffset stream rows"):
        """Wait until the sink holds exactly the rows emitted for `epochs`.

        Rows are filtered by timestamp so canary windows emitted before the readiness proof
        cannot pollute the comparison, and a transient query error is retried.
        """
        deadline = time.monotonic() + timeout
        actual = None
        last_error = None
        while time.monotonic() < deadline:
            try:
                actual = [tuple(row[1:]) for row in cls._dto_sink_rows(epochs)]
            except Exception as error:
                last_error = error
                time.sleep(0.5)
                continue
            if actual == expected:
                return
            time.sleep(0.5)
        raise AssertionError(f"{reason}: expected={expected}, actual={actual}, error={last_error}")

    @classmethod
    def _wait_dto_reader_ready(cls, scale, attempts=8, per_attempt=20):
        """Prove the external reader already sampled its history cutoff.

        checkStreamStatus() only reports that the START request was queued; the reader
        samples its history cutoff afterwards and rows written before that sample are
        consumed as history and never emitted.  A canary row at a fresh timestamp proves
        the sample happened; while it is still pending the attempt is retried.  Epochs are
        one second apart so a zero-scale datetimeoffset column keeps them distinct.
        """
        value = 900
        for attempt in range(attempts):
            value += 1
            epoch = cls.BASE_US + value * 1000000
            cls._insert_dto(epoch, value, scale)
            try:
                cls._wait_dto_rows([epoch], [(value, 1)], timeout=per_attempt, reason="reader readiness")
                tdLog.info(f"datetimeoffset reader is ready after {attempt + 1} canary attempt(s)")
                return
            except AssertionError as error:
                tdLog.info(f"datetimeoffset reader readiness canary not observed yet: {error}")
        raise AssertionError(
            "external reader never emitted a canary row: its history cutoff was not sampled"
        )

    def do_sqlserver_datetimeoffset(self, scale, timezone="Asia/Shanghai", epochs=None):
        """datetimeoffset time axis keeps absolute instants when the stream tz is not UTC.

        1. Create a datetimeoffset(scale) time axis and a virtual stream whose creating
           session uses Asia/Shanghai, so the deployed plan records that timezone.
        2. Prove the reader sampled its history cutoff with a canary row.
        3. Insert rows carrying an explicit ``+00:00`` offset and verify every sink row
           lands on its absolute epoch.

        ``datetimeoffset`` holds an absolute instant: the connector converts it through its
        own offset and the reader must render the metadata watermark and the trigger fetch
        bounds in UTC.  Rendering them as an offset-less wall clock in the client timezone
        shifts every bound by the offset (eight hours for Asia/Shanghai); SQL Server reads
        such a literal as ``+00:00``, so rows just past the watermark are filtered out and
        the window fetch returns nothing.  The reader has to recover both the stream
        timezone and the timestamp class from its virtual-table deploy plan, which carries a
        plain table scan instead of a federated scan; the non-UTC server-global case is
        covered by the streamReaderTest unit tests.  A zero scale also exercises the
        connector's parsing of a ``datetimeoffset`` text without a fractional part.
        """
        self._cleanup()
        try:
            create_remote_db("sqlserver", self.REMOTE_DB)
            create_sqlserver_table(
                self.REMOTE_DB, "dto_t",
                f"ts datetimeoffset({scale}) NOT NULL PRIMARY KEY, v INT",
                [f"('{self._dto_literal(self.BASE_US, scale)}', 0)"],
            )
            create_ext_source(self.SOURCE, "sqlserver", self.REMOTE_DB)
            tdSql.execute(f"CREATE DATABASE {self.DB} VGROUPS 1 PRECISION 'us'")
            tdSql.execute(
                f"CREATE STABLE {self.DB}.vstb (ts TIMESTAMP, value_a INT) "
                "TAGS (site INT) VIRTUAL 1"
            )
            tdSql.execute(
                f"CREATE VTABLE {self.DB}.vt ("
                f"value_a FROM {self.SOURCE}.dbo.dto_t.v) "
                f"USING {self.DB}.vstb TAGS(1)"
            )
            # The stream plan captures the creating session's timezone; the plan is
            # what the reader uses to render its remote bounds at runtime.
            tdSql.execute(f"set timezone '{timezone}'")
            try:
                tdSql.execute(
                    f"CREATE STREAM {self.DB}.{self.STREAM} COUNT_WINDOW(1) "
                    f"FROM {self.DB}.vstb PARTITION BY tbname "
                    f"INTO {self.DB}.sink (first_ts, value_a, row_count) "
                    "AS SELECT first(_c0), sum(value_a), count(*) FROM %%trows"
                )
            finally:
                # Keep the shared connection on UTC for assertions and later cases.
                tdSql.execute("set timezone 'UTC'")
            tdStream.checkStreamStatus(self.STREAM)
            self._wait_dto_reader_ready(scale)

            # The assertion rows must be newer than the canary rows: the stream does not
            # replay history, so a watermark already advanced past them would drop them.
            epochs = epochs or [self.BASE_US + 1000 * 1000000, self.BASE_US + 1001 * 1000000]
            for epoch, value in zip(epochs, (11, 12)):
                self._insert_dto(epoch, value, scale)
            self._wait_dto_rows(epochs, [(11, 1), (12, 1)])
            tdLog.info(f"SQL Server datetimeoffset({scale}) virtual stream [ passed ]")
        finally:
            self._cleanup()

    def do_sqlserver_virtual_stream(self, mixed):
        self._cleanup()
        try:
            create_remote_db("sqlserver", self.REMOTE_DB)
            # Naive SQL Server timestamps are decoded in the stream timezone (the
            # creating session is on UTC), so spell the UTC wall clock here.
            # Insert local TDengine rows as epochs so both sources share one time axis.
            seed = self._remote_stamp(self.BASE_US)
            for schema in ("dbo", "other"):
                create_sqlserver_table(
                    self.REMOTE_DB, "same_t",
                    "ts datetime2(6) NOT NULL PRIMARY KEY, v INT",
                    [f"('{seed.isoformat(timespec='microseconds')}', 0)"], schema=schema,
                )
            create_ext_source(self.SOURCE, "sqlserver", self.REMOTE_DB)
            tdSql.execute(f"CREATE DATABASE {self.DB} VGROUPS 1 PRECISION 'us'")
            tdSql.execute(f"CREATE TABLE {self.DB}.local_t (ts TIMESTAMP, v INT)")
            tdSql.execute(f"INSERT INTO {self.DB}.local_t VALUES ({self.BASE_US}, 0)")
            tdSql.execute(
                f"CREATE STABLE {self.DB}.vstb "
                "(ts TIMESTAMP, value_a INT, value_b INT, local_value INT) "
                "TAGS (site INT) VIRTUAL 1"
            )
            local_ref = f", local_value FROM {self.DB}.local_t.v" if mixed else ""
            # Exercise both default-database and explicit-database reference paths.
            ref_a = f"{self.SOURCE}.{self.REMOTE_DB}.dbo" if mixed else f"{self.SOURCE}.dbo"
            ref_b = f"{self.SOURCE}.other" if mixed else f"{self.SOURCE}.{self.REMOTE_DB}.other"
            tdSql.execute(
                f"CREATE VTABLE {self.DB}.vt ("
                f"value_a FROM {ref_a}.same_t.v, "
                f"value_b FROM {ref_b}.same_t.v"
                f"{local_ref}) USING {self.DB}.vstb TAGS(1)"
            )
            tdLog.debug(f"creating SQL Server virtual stream, mixed={mixed}")
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
            for schema, values in (("dbo", (11, 12, 13)), ("other", (21, 22, 23))):
                rows = ", ".join(
                    f"('{self._remote_stamp(epoch).isoformat(timespec='microseconds')}', {value})"
                    for epoch, value in zip(epochs, values)
                )
                ExtSrcEnv.sqlserver_exec(
                    self.REMOTE_DB, [f"INSERT INTO {schema}.same_t VALUES {rows}"]
                )
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
                tdLog.debug("rebinding SQL Server virtual column through a short reference")
                tdSql.execute(
                    f"ALTER VTABLE {self.DB}.vt ALTER COLUMN value_b SET {self.SOURCE}.dbo.same_t.v"
                )
                tdStream.checkStreamStatus(self.STREAM)
                # A row written only to dbo is the rebind probe: value_b reads 41 only once the new
                # binding is deployed; while the patch is pending it still reads the old binding
                # (other.same_t) and stays NULL. Keep every attempt for the final history check,
                # including windows emitted through the old binding.
                probe_epoch = None
                probe_epochs = []
                for attempt in range(8):
                    epoch = self.BASE_US + 10654321 + attempt
                    self._insert_remote("dbo", epoch, 41)
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
                # Rebinding must preserve completed windows, including their local values.
                self._wait_rows(
                    epochs + [probe_epoch],
                    expected + [(self._stamp(probe_epoch), 41, 41, 61, 1)],
                    reason="rebind history",
                )
                self._check_rebind_probe_history(probe_epochs)
            tdLog.info(f"SQL Server virtual stream, mixed={mixed} [ passed ]")
        finally:
            self._cleanup()

    def test_sqlserver_external_columns(self):
        """SQL Server virtual stream preserves schema identity and microseconds.

        1. Reference same-named tables in dbo and a non-default schema.
        2. Check every output row, including timestamps one microsecond apart.

        Catalog:
            - Stream

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-20 Wang Mingming Add SQL Server virtual stream coverage.
        """
        self.do_sqlserver_virtual_stream(False)

    def test_sqlserver_mixed_local_columns(self):
        """SQL Server and local columns merge into virtual stream rows.

        1. Combine local WAL data with two SQL Server schema bindings.
        2. Verify timestamp alignment, values, and row counts in the sink.
        3. Rebind through a short SQL Server path and verify historical and new rows.

        Catalog:
            - Stream

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-20 Wang Mingming Add mixed SQL Server stream coverage.
        """
        self.do_sqlserver_virtual_stream(True)

    def test_sqlserver_sub_microsecond_time_axis_rejected(self):
        """SQL Server virtual stream rejects a time axis finer than a microsecond.

        1. Reference a datetime2(7) time axis that holds 2026-01-01 00:00:01.1234567.
        2. Verify the virtual table itself stays queryable (predicates are approximated).
        3. Verify the stream fails with the precision error and emits nothing, instead of
           silently dropping rows whose seventh fractional digit is non-zero.

        The reader advances a microsecond watermark over the external timestamp while the
        connector truncates the seventh digit, so a row that only differs below one
        microsecond can neither be distinguished from the consumed row nor be skipped
        safely. The vtable remains legal; only the stream binding is rejected.

        Catalog:
            - Stream

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-22 Wang Mingming Reject SQL Server sub-microsecond stream time axis.
        """
        self._cleanup()
        try:
            create_remote_db("sqlserver", self.REMOTE_DB)
            create_sqlserver_table(
                self.REMOTE_DB, "fine_t",
                "ts datetime2(7) NOT NULL PRIMARY KEY, v INT",
                ["('2026-01-01 00:00:01.1234567', 7)"],
            )
            create_ext_source(self.SOURCE, "sqlserver", self.REMOTE_DB)
            tdSql.execute(f"CREATE DATABASE {self.DB} VGROUPS 1 PRECISION 'us'")
            tdSql.execute(
                f"CREATE STABLE {self.DB}.vstb (ts TIMESTAMP, value_a INT) "
                "TAGS (site INT) VIRTUAL 1"
            )
            tdSql.execute(
                f"CREATE VTABLE {self.DB}.vt ("
                f"value_a FROM {self.SOURCE}.dbo.fine_t.v) "
                f"USING {self.DB}.vstb TAGS(1)"
            )
            # The virtual table keeps accepting datetime2(7): its predicates are
            # approximated to microseconds and the vtable DDL stays unchanged.
            tdSql.query(f"SELECT value_a FROM {self.DB}.vt")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 7)

            create_stream_sql = (
                f"CREATE STREAM {self.DB}.{self.STREAM} COUNT_WINDOW(1) "
                f"FROM {self.DB}.vstb PARTITION BY tbname "
                f"INTO {self.DB}.sink (first_ts, value_a, row_count) "
                "AS SELECT first(_c0), sum(value_a), count(*) FROM %%trows"
            )
            try:
                tdSql.execute(create_stream_sql)
            except Exception as error:
                # A synchronous rejection is acceptable as long as it names the cause.
                assert "microsecond" in str(error), error
            else:
                self._wait_stream_failed(self.STREAM, "microsecond")

            tdSql.query(
                "SELECT count(*) FROM information_schema.ins_tables "
                f"WHERE db_name='{self.DB}' AND table_name='sink'")
            if tdSql.getData(0, 0) > 0:
                tdSql.query(f"SELECT count(*) FROM {self.DB}.sink")
                tdSql.checkData(0, 0, 0)
            tdLog.info("SQL Server sub-microsecond virtual stream time axis was rejected")
        finally:
            self._cleanup()

    def test_sqlserver_datetimeoffset_non_utc_timezone(self):
        """datetimeoffset(6) time axis keeps absolute instants for a non-UTC stream.

        1. Create a datetimeoffset(6) time axis and a virtual stream whose creating
           session uses Asia/Shanghai, so the deployed plan records that timezone.
        2. Prove the reader sampled its history cutoff with a canary row.
        3. Insert rows that carry an explicit ``+00:00`` offset.
        4. Verify every sink row lands on its absolute epoch.

        This case is the non-UTC counterpart of the ``datetime2`` coverage above and
        only passes when the reader classifies the time axis as timezone-aware, which
        for a virtual table requires recovering the class from the deploy spec.

        Catalog:
            - Stream

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-22 Wang Mingming Cover datetimeoffset bounds on a non-UTC stream.
        """
        self.do_sqlserver_datetimeoffset(6)

    def test_sqlserver_datetimeoffset_zero_scale(self):
        """datetimeoffset(0) keeps its explicit offset in the connector.

        1. Create a datetimeoffset(0) time axis, whose ODBC text carries no fractional
           part, plus a virtual stream created in Asia/Shanghai.
        2. Prove the reader sampled its history cutoff with a canary row.
        3. Insert rows at absolute ``+00:00`` instants and verify their epochs.

        The connector used to require a fractional part before the offset, so a
        zero-scale value such as ``2026-01-01 08:00:00 +08:00`` silently lost its offset
        and was decoded in the client timezone.

        Catalog:
            - Stream

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-22 Wang Mingming Cover datetimeoffset(0) offset parsing.
        """
        self.do_sqlserver_datetimeoffset(0)

    @staticmethod
    def _utc_epoch_us(year, month, day, hour, minute, second=0, microsecond=0):
        """Epoch microseconds of a UTC wall clock (the offset a +00:00 literal names)."""
        stamp = datetime.datetime(year, month, day, hour, minute, second, microsecond,
                                  tzinfo=datetime.timezone.utc)
        return int(stamp.timestamp()) * 1000000 + stamp.microsecond

    def test_sqlserver_datetimeoffset_dst_gap(self):
        """datetimeoffset values stay absolute when their wall clock is a DST gap.

        1. Create a datetimeoffset(6) time axis and a virtual stream whose creating
           session uses America/New_York.
        2. Prove the reader sampled its history cutoff with a canary row.
        3. Insert rows whose *wall clock* is ``2026-03-08 02:15/02:45 +00:00``.
           That wall clock does not exist in New York (02:00-03:00 is skipped on
           that day), yet the value itself is a perfectly valid instant.
        4. Verify both rows land on their absolute epochs.

        The connector used to convert every temporal text through the client
        timezone first and only then apply an explicit offset.  A datetimeoffset
        whose wall clock falls into the stream's DST gap therefore failed with
        ``TSDB_CODE_EXT_TYPE_CONVERT_FAILED`` before the offset was even read, which
        also broke the initial MAX(ts) sampling of the stream.  The offset must be
        applied without consulting the client timezone.

        Catalog:
            - Stream

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-23 Wang Mingming Cover datetimeoffset values inside a DST gap.
        """
        self.do_sqlserver_datetimeoffset(
            6,
            timezone="America/New_York",
            epochs=[
                self._utc_epoch_us(2026, 3, 8, 2, 15),
                self._utc_epoch_us(2026, 3, 8, 2, 45),
            ],
        )
