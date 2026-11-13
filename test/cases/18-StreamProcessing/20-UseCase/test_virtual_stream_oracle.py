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


class PartialCommit(Exception):
    """A window closed before every row of its timestamp had been committed.

    Only this failure is worth retrying with fresh timestamps: a query can return a row whose
    source columns are still missing, and retrying cannot repair a published wrong value.
    """


class TestVirtualStreamOracle:
    DB = "vs_oracle"
    SOURCE = "vs_oracle_src"
    STREAM = "oracle_stream"
    BASE_US = 1767225600000000

    # A measurement timestamp can become visible to the trigger before every source
    # of that timestamp committed (each source commits on its own connection), so the
    # measurement is retried at fresh timestamps instead of asserting a partial row.
    _MEASURE_ATTEMPTS = 4
    # One attempt writes the epochs base, base + 1 and base + `_MEASURE_SPAN_US`, so the
    # next attempt has to start past the previous attempt's last timestamp: an attempt
    # that closed a window on a partially committed row has already advanced the
    # reader watermark and the reader only scans rows past it.  The step stays below
    # (rebind probe offset - span) / attempts so the last attempt still ends before the
    # rebind probes.  setup_class() asserts this layout (span < step, and
    # attempts x step + span < probe offset) instead of leaving it to this comment, so
    # changing any of the four numbers fails fast rather than silently overlapping the
    # retry windows with each other or with the probes.
    _MEASURE_SPAN_US = 1_530_865
    _MEASURE_STEP_US = 2_000_000
    # The rebind probes start here instead of right after the measurement: the reader only
    # scans past its watermark, so a probe written too early would be swallowed by a
    # window that closed on a partially committed retry.
    _REBIND_PROBE_OFFSET_US = 10_654_321

    # Oracle has one namespace level - the schema - and the fixture accounts own
    # no cross-schema grants, so the remote tables live in the login user's own
    # schema.  An unquoted identifier is lower-cased by TDengine, so every path
    # spells the upper-case account name between backticks.
    SCHEMA = ExtSrcEnv.ORACLE_USER.upper()
    TABLE_A = "SAME_A"
    TABLE_B = "SAME_B"

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        # The retry layout below only holds while an attempt ends before the next one starts
        # and the last attempt stays clear of the rebind probes: assert both here so a later
        # tweak of the four numbers breaks immediately instead of turning into an
        # intermittent assertion.
        assert cls._MEASURE_SPAN_US < cls._MEASURE_STEP_US, (
            f"retry attempts overlap: span {cls._MEASURE_SPAN_US} >= "
            f"step {cls._MEASURE_STEP_US}")
        assert (cls._MEASURE_ATTEMPTS * cls._MEASURE_STEP_US + cls._MEASURE_SPAN_US
                < cls._REBIND_PROBE_OFFSET_US), (
                    "measurement retries overlap the rebind probes: "
                    f"{cls._MEASURE_ATTEMPTS} x {cls._MEASURE_STEP_US} + "
                    f"{cls._MEASURE_SPAN_US} >= {cls._REBIND_PROBE_OFFSET_US}")
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

    @staticmethod
    def _row_mismatch(actual, expected):
        """Classify how the observed sink rows differ from `expected`.

        Rows are filtered by timestamp, so every observed row belongs to one of the expected
        windows.  The verdict says what the caller may do with the difference:

        - ``"arriving"``: nothing contradicts `expected` yet, but a window has not fired (or is
          still empty), so waiting may still produce the expected rows.
        - ``"partial"``: every fired window carries the right ``value_a`` and ``row_count`` and
          only lacks a value.  Each source commits on its own connection, so this is the one
          shape a late commit explains, and the only one a retry with fresh timestamps may
          paper over.
        - ``"wrong"``: a published non-NULL value, count or row multiplicity that `expected`
          rules out.  Commit order cannot rewrite a value that was already emitted, so a retry
          must not hide it.
        """
        grouped = {}
        for row in actual:
            grouped.setdefault(row[0], []).append(row)
        partial = False
        arriving = False
        for want in expected:
            rows = grouped.get(want[0])
            if not rows:
                arriving = True
                continue
            if len(rows) > 1:
                return "wrong"
            have = rows[0]
            if have[1] != want[1] or have[4] != want[4]:
                return "wrong"
            for idx in (2, 3):
                if have[idx] == want[idx]:
                    continue
                if have[idx] is None:
                    partial = True
                    continue
                return "wrong"
        if partial:
            return "partial"
        return "arriving" if arriving else "match"

    @classmethod
    def _wait_rows(cls, epochs, expected, timeout=120, reason="Oracle stream rows",
                   allow_partial=False):
        """Wait until the sink holds exactly the rows emitted for `epochs`.

        Rows are filtered by timestamp so windows emitted by an earlier probe attempt (possibly
        through a stale binding) cannot pollute the comparison, and a transient query error is
        retried instead of failing the case.  A mismatch that no commit order explains fails
        immediately: waiting cannot turn a published wrong value into the right one.  With
        `allow_partial` an attempt that only ever showed the partially committed shape raises
        `PartialCommit`, which the measurement loop retries.
        """
        deadline = time.monotonic() + timeout
        actual = None
        last_error = None
        partial = False
        wrong = None
        wrong_seen = 0
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
            verdict = cls._row_mismatch(actual, expected)
            if verdict == "wrong":
                # Confirm the same rows twice so a row still being rewritten cannot trip this.
                if actual == wrong:
                    wrong_seen += 1
                else:
                    wrong, wrong_seen = actual, 1
                if wrong_seen >= 2:
                    raise AssertionError(
                        f"{reason}: a window published rows no commit order explains: "
                        f"expected={expected}, actual={actual}")
            elif verdict == "partial":
                partial = True
            else:
                wrong, wrong_seen = None, 0
            time.sleep(0.5)
        if partial and allow_partial:
            raise PartialCommit(f"{reason}: only partially committed rows visible: {actual}")
        raise AssertionError(f"{reason}: expected={expected}, actual={actual}, error={last_error}")

    @classmethod
    def _check_measurement_history(cls, attempts, timeout=60):
        """Check every measurement window that fired, including the retried attempts.

        The retry loop only inspects the window it is waiting for, and each attempt keeps its
        own timestamps, so a wrong value published by an earlier attempt would go unread.  Such
        a window may still be short of a value (``value_b`` or ``local_value`` are NULL when
        their source had not committed yet), but a wrong non-NULL value, a different
        ``row_count``, a window emitted twice or a window that never fired is a defect that the
        retry must not hide: TABLE_A is committed before the local row is written, so every
        measurement timestamp must close its COUNT_WINDOW(1) window.
        """
        epochs = [epoch for attempt_epochs, _ in attempts for epoch in attempt_epochs]
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
            "measurement windows never appeared: "
            f"missing={sorted(missing)}, rows={rows}, error={last_error}"
        )
        grouped = {}
        for row in rows:
            grouped.setdefault(row[0], []).append(row)
        for _, attempt_expected in attempts:
            for want in attempt_expected:
                got = grouped.get(want[0], [])
                assert len(got) <= 1, (
                    f"measurement window {want[0]} was emitted {len(got)} times: {got}")
                have = got[0]
                assert have[1] == want[1] and have[4] == want[4], (
                    f"measurement window {want[0]} published {have}, but its rows carry "
                    f"value_a={want[1]} and row_count={want[4]}")
                assert have[2] in (want[2], None) and have[3] in (want[3], None), (
                    f"measurement window {want[0]} published {have}; only a missing "
                    f"value_b/local_value is explained by a late commit")

    @classmethod
    def _check_rebind_probe_history(cls, epochs, timeout=60):
        """Every probe attempt must have produced a window without a wrong value.

        The successful attempt is already awaited by the caller; earlier attempts may still be in
        flight, so wait until each probe epoch has at least one row before checking values.  Every
        probe commits SAME_A first and only then writes the local row, so a window that closed on
        an earlier probe already had value_a; a NULL value_a therefore proves data loss rather
        than commit order.  value_b may be NULL while the ALTER is still pending (the window then
        read the old binding, which has no row at that timestamp) and local_value may be NULL when
        the window closed between the Oracle commit and the local insert.
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
            assert (row[1] == 41 and row[2] in (None, 41) and row[3] in (None, 61)
                    and row[4] == 1), (
                f"Rebind corrupted a completed probe window: {tuple(row)}"
            )

    @classmethod
    def _insert_remote(cls, table, epoch_us, value):
        stamp = cls._oracle_ts_literal(cls._remote_stamp(epoch_us))
        cfg = next(ExtSrcEnv.oracle_version_configs())
        # Oracle has no multi-row VALUES and SQL*Plus does not commit on exit, so
        # the INSERT and COMMIT share one sqlplus session via a batch.
        # create_oracle_table stores quoted lower-case names, and Oracle
        # matches exact spelling, so the INSERT must quote the lower-case
        # table name instead of relying on unquoted upper-case folding.
        ExtSrcEnv._oracle_sql(
            cfg, [f'INSERT INTO "{table.lower()}" VALUES '
                  f"(TIMESTAMP '{stamp}', {value})", "COMMIT"]
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
            # TDengine lower-cases an unquoted identifier, so the upper-case
            # account name in the schema segment is spelled between backticks;
            # the fixture stores the quoted lower-case table names.
            ref_a = f"{self.SOURCE}.`{self.SCHEMA}`.{self.TABLE_A}"
            ref_b = f"{self.SOURCE}.`{self.SCHEMA}`.{self.TABLE_B}"
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
            epochs = None
            expected = None
            # Every source commits on its own connection, so a timestamp becomes
            # visible to the trigger as soon as its first source commits and the
            # COUNT window may legitimately close while the other columns are still
            # missing (a virtual column whose source has no row for that timestamp
            # reads NULL).  Cross-database atomicity cannot be produced from here:
            # write one epoch across all sources at a time (the shortest possible
            # gap) and treat an attempt whose window closed on a partially
            # committed timestamp as inconclusive, retrying with fresh timestamps.
            # Only that shape is retried: a wrong value or count fails right away,
            # and _check_measurement_history() re-reads every attempt afterwards.
            attempts = []
            for attempt in range(self._MEASURE_ATTEMPTS):
                base = self.BASE_US + (attempt + 1) * self._MEASURE_STEP_US
                epochs = [base, base + 1, base + self._MEASURE_SPAN_US]
                for epoch, value_a, value_b, local_value in zip(
                        epochs, (11, 12, 13), (21, 22, 23), (31, 32, 33)):
                    self._insert_remote(self.TABLE_A, epoch, value_a)
                    self._insert_remote(self.TABLE_B, epoch, value_b)
                    if mixed:
                        tdSql.execute(
                            f"INSERT INTO {self.DB}.local_t VALUES ({epoch}, {local_value})")
                expected = [
                    (self._stamp(epochs[0]), 11, 21, 31 if mixed else None, 1),
                    (self._stamp(epochs[1]), 12, 22, 32 if mixed else None, 1),
                    (self._stamp(epochs[2]), 13, 23, 33 if mixed else None, 1),
                ]
                attempts.append((epochs, expected))
                try:
                    self._wait_rows(epochs, expected, timeout=30, reason="measurement",
                                    allow_partial=True)
                    break
                except PartialCommit as error:
                    tdLog.info(
                        f"measurement attempt {attempt + 1}/{self._MEASURE_ATTEMPTS} "
                        f"closed a window on a partially committed timestamp: {error}")
                    if attempt + 1 >= self._MEASURE_ATTEMPTS:
                        raise AssertionError(
                            f"every measurement attempt closed on a partial commit: {error}"
                        ) from error
            self._check_measurement_history(attempts)
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
                    epoch = self.BASE_US + self._REBIND_PROBE_OFFSET_US + attempt
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

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7132783339

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

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7132783339

        History:
            - 2026-11-02 Wang Mingming Add mixed Oracle stream coverage.
        """
        self.do_oracle_virtual_stream(True)
