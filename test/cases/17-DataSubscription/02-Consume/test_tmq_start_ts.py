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

import ctypes
import json
import taos
import time
from datetime import datetime

from taos.tmq import Consumer
from taos.cinterface import _libtaos
from taos.error import TmqError

from new_test_framework.utils import tdLog, tdSql

# driver converts TIMESTAMP columns to naive local datetime objects via
# datetime.fromtimestamp(0) + timedelta; convert back to epoch ms with pure arithmetic
_TS_EPOCH = datetime.fromtimestamp(0)


def ts_to_ms(value):
    if isinstance(value, datetime):
        return int((value - _TS_EPOCH).total_seconds() * 1000)
    return int(value)


class RawReplay:
    """ctypes wrapper for the raw-data replay API (tmq_get_raw / tmq_write_raw)."""

    class RawData(ctypes.Structure):
        _fields_ = [
            ("raw", ctypes.c_void_p),
            ("raw_len", ctypes.c_uint32),
            ("raw_type", ctypes.c_uint16),
        ]

    def __init__(self):
        self._get_raw = _libtaos.tmq_get_raw
        self._get_raw.argtypes = [ctypes.c_void_p, ctypes.POINTER(RawReplay.RawData)]
        self._get_raw.restype = ctypes.c_int32
        self._write_raw = _libtaos.tmq_write_raw
        self._write_raw.argtypes = [ctypes.c_void_p, RawReplay.RawData]
        self._write_raw.restype = ctypes.c_int32
        self._free_raw = _libtaos.tmq_free_raw
        self._free_raw.argtypes = [RawReplay.RawData]
        self._free_raw.restype = None

    def write(self, connection, result):
        """Replay one TMQ response into the target and return tmq_write_raw's code."""
        raw = RawReplay.RawData()
        assert self._get_raw(result.msg, ctypes.byref(raw)) == 0
        try:
            return self._write_raw(connection._conn, raw)
        finally:
            self._free_raw(raw)


def json_meta_text(msg):
    """Return tmq_get_json_meta() text for a TMQ poll result, None for data messages."""
    _libtaos.tmq_get_json_meta.argtypes = [ctypes.c_void_p]
    _libtaos.tmq_get_json_meta.restype = ctypes.c_void_p
    _libtaos.tmq_free_json_meta.argtypes = [ctypes.c_void_p]
    _libtaos.tmq_free_json_meta.restype = None
    ptr = _libtaos.tmq_get_json_meta(msg.msg)
    if not ptr:
        return None
    try:
        return ctypes.cast(ptr, ctypes.c_char_p).value.decode("utf-8", errors="replace")
    finally:
        _libtaos.tmq_free_json_meta(ptr)



class TestCase:
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def consume_rows(self, consumer, expected_rows, timeout_seconds=30):
        rows = []
        deadline = time.time() + timeout_seconds
        while len(rows) < expected_rows and time.time() < deadline:
            result = consumer.poll(1)
            if not result:
                continue
            error = result.error()
            if error is not None:
                tdLog.exit(f"TMQ poll failed: {error}")
            value = result.value()
            if value is None:  # meta message, not counted as data
                continue
            for block in value:
                rows.extend(block.fetchall())
        if len(rows) != expected_rows:
            tdLog.exit(f"Expected {expected_rows} TMQ rows, got {len(rows)}")
        return rows

    def do_filtered_create_table_metadata(self):
        database = "db_start_ts_meta"
        topic = "topic_start_ts_meta"
        consumer = None
        get_meta = _libtaos.tmq_get_json_meta
        get_meta.argtypes = [ctypes.c_void_p]
        get_meta.restype = ctypes.c_void_p
        free_meta = _libtaos.tmq_free_json_meta
        free_meta.argtypes = [ctypes.c_void_p]
        free_meta.restype = None
        try:
            tdSql.execute(f"drop database if exists {database}")
            tdSql.execute(f"create database {database} vgroups 1 wal_retention_period 3600")
            tdSql.execute(f"create stable {database}.stb (ts timestamp, val int) tags(t1 int)")
            tdSql.execute(f"create topic {topic} with meta as database {database}")
            consumer = Consumer({
                "group.id": "g_start_ts_meta",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": "false",
                "msg.consume.start.ts": "1640966400050",
            })
            consumer.subscribe([topic])
            tdSql.execute(
                f"insert into {database}.ct_filtered using {database}.stb tags(1) "
                "values (1640966400000, 1)"
            )
            found = False
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                result = consumer.poll(1)
                if result is None:
                    continue
                assert result.error() is None, result.error()
                pointer = get_meta(result.msg)
                if pointer:
                    try:
                        metadata = ctypes.string_at(pointer).decode("utf-8")
                        found = '"ct_filtered"' in metadata
                    finally:
                        free_meta(pointer)
                assert not result.value(), "Filtered INSERT unexpectedly delivered data"
                if found:
                    break
            assert found, "Filtered INSERT lost its child-table creation metadata"
            tdLog.info("Filtered INSERT preserved child-table metadata")
        finally:
            if consumer is not None:
                consumer.close()
            tdSql.execute(f"drop topic if exists {topic}")
            tdSql.execute(f"drop database if exists {database}")

    def do_filtered_create_table_metadata_batches(self, raw_data=False):
        database = "db_start_ts_meta_batch"
        topic = "topic_start_ts_meta_batch"
        names = [f"ct_batch_{i}" for i in range(3)]
        consumer = connection = None
        target = "db_start_ts_meta_batch_dst"
        try:
            if raw_data:
                tdSql.execute(f"drop database if exists {target}")
                tdSql.execute(f"create database {target} vgroups 1 keep 36500")
                connection = taos.connect(database=target)
                replay = RawReplay()
            tdSql.execute(f"drop database if exists {database}")
            tdSql.execute(f"create database {database} vgroups 1 wal_retention_period 3600")
            tdSql.execute(f"create stable {database}.stb (ts timestamp, val int) tags(t1 int)")
            tdSql.execute(f"create topic {topic} with meta as database {database}")
            consumer = Consumer({
                "group.id": "g_start_ts_meta_batch",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": "false",
                # Metadata-only responses carry no rows, so this batch size must also bound the
                # pending create-table metadata (see tqExtractDataAndRspForDbStbSubscribe).
                "min.poll.rows": "1",
                "msg.consume.rawdata": "1" if raw_data else "0",
                "experimental.snapshot.enable": "false",
                "msg.consume.start.ts": "1640966400050",
            })
            consumer.subscribe([topic])
            for i, name in enumerate(names):
                tdSql.execute(
                    f"insert into {database}.{name} using {database}.stb tags({i}) "
                    f"values (1640966400000, {i})"
                )

            pending = set(names)
            responses = []
            child_batches = []
            deadline = time.monotonic() + 30
            while pending and time.monotonic() < deadline:
                result = consumer.poll(1)
                if result is None:
                    continue
                assert result.error() is None, result.error()
                if raw_data:
                    assert replay.write(connection, result) == 0
                else:
                    assert not result.value(), "Filtered INSERT unexpectedly delivered data"
                text = json_meta_text(result)
                if text is None:
                    continue
                meta = json.loads(text)
                created = [meta.get("tableName")]
                created += [item.get("tableName") for item in meta.get("createList") or []]
                # The top-level tableName repeats the first createList entry.
                created = list(dict.fromkeys(name for name in created if name))
                assert created, meta
                responses.append(created)
                if any(name in names for name in created):
                    child_batches.append(created)
                pending.difference_update(created)
            assert not pending, f"lost create-table metadata for {sorted(pending)}"
            # min.poll.rows=1 must bound the pending metadata: every filtered INSERT delivers its
            # create-table metadata in its own response instead of accumulating until EOF/timeout.
            assert len(child_batches) >= len(names), responses
            assert all(len(batch) == 1 for batch in child_batches), responses
            if raw_data:
                tdSql.query(f"select val from {target}.stb order by val")
                assert [row[0] for row in tdSql.queryResult] == list(range(len(names))), tdSql.queryResult
            tdLog.info(f"Filtered auto-create metadata delivered in {len(responses)} batches")
        finally:
            if consumer is not None:
                consumer.close()
            if connection is not None:
                connection.close()
            tdSql.execute(f"drop database if exists {target}")
            tdSql.execute(f"drop topic if exists {topic}")
            tdSql.execute(f"drop database if exists {database}")

    def test_rawdata_create_table_metadata_batches(self):
        """Bound rawdata auto-create metadata and preserve rows before start.ts in WAL.

        1. Consume a database topic with metadata, raw data and a start timestamp.
        2. Replay three auto-create INSERTs whose rows are all filtered out by start.ts.
        3. Verify each INSERT delivers its own create-table metadata response.
        4. Verify the replayed rows filtered out of the data stream are still written to WAL.

        Catalog:
            - DataSubscription

        Since: v3.4.3.6

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-10-23 Added rawdata fallback regression

        """
        self.do_filtered_create_table_metadata_batches(raw_data=True)

    def do_invalid_start_ts(self):
        invalid_values = (
            ("-1", "negative integer"),
            ("-1000", "negative epoch in milliseconds"),
            ("1.5", "fractional value"),
            ("1640995200050abc", "trailing characters"),
            ("2022-01-01 00:00:00.050+00:00", "datetime string"),
            ("2022-01-01T00:00:00.050Z", "datetime string"),
            ("not-a-timestamp", "unparsable string"),
        )
        for idx, (value, reason) in enumerate(invalid_values):
            consumer = None
            try:
                consumer = Consumer({
                    "group.id": f"g_start_ts_reject_{idx}",
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": "false",
                    "msg.consume.start.ts": value,
                })
            except TmqError as err:
                tdLog.info(f"msg.consume.start.ts={value} rejected as expected ({reason}): {err}")
            else:
                tdLog.exit(
                    f"msg.consume.start.ts={value} ({reason}) must be rejected, but the consumer was created"
                )
            finally:
                if consumer is not None:
                    consumer.close()

    def do_start_ts_precision(self):
        # A non-negative threshold still has to be converted from milliseconds to the
        # database precision (ms/us/ns), on both the WAL and the snapshot path.
        base_ts = 1640995200000  # 2022-01-01 00:00:00.000 UTC
        for precision, scale in (("ms", 1), ("us", 1000), ("ns", 1000000)):
            for snapshot in ("false", "true"):
                database = f"db_start_ts_prec_{precision}_{snapshot}"
                topic = f"topic_start_ts_prec_{precision}_{snapshot}"
                consumer = None
                try:
                    tdSql.execute(f"drop database if exists {database}")
                    tdSql.execute(
                        f"create database {database} precision '{precision}' keep 36500 "
                        "vgroups 1 wal_retention_period 3600"
                    )
                    tdSql.execute(f"create stable {database}.stb (ts timestamp, val int) tags(t1 int)")
                    tdSql.execute(f"create table {database}.ct using {database}.stb tags(1)")
                    tdSql.execute(
                        f"insert into {database}.ct values ({(base_ts - 2000) * scale}, 0) "
                        f"({(base_ts - 1000) * scale}, 1) ({(base_ts - 999) * scale}, 2) "
                        f"({(base_ts + 1) * scale}, 3)"
                    )
                    if snapshot == "true":
                        tdSql.execute(f"flush database {database}")
                    tdSql.execute(f"create topic {topic} as stable {database}.stb")
                    consumer = Consumer({
                        "group.id": f"g_start_ts_prec_{precision}_{snapshot}",
                        "auto.offset.reset": "earliest",
                        "enable.auto.commit": "false",
                        "experimental.snapshot.enable": snapshot,
                        "msg.consume.start.ts": str(base_ts - 1000),
                    })
                    consumer.subscribe([topic])
                    rows = self.consume_rows(consumer, 3)
                    assert [row[1] for row in rows] == [1, 2, 3], rows
                    tdLog.info(f"Start timestamp precision verified: precision={precision}, snapshot={snapshot}")
                finally:
                    if consumer is not None:
                        consumer.close()
                    tdSql.execute(f"drop topic if exists {topic}")
                    tdSql.execute(f"drop database if exists {database}")

    def test_start_ts_varchar(self):
        """Keep variable-length columns intact when a block is partially filtered.

        1. Insert rows with a varchar column (including NULL values) in a single statement so
           one block spans the start timestamp.
        2. Consume on the WAL and the snapshot path with msg.consume.start.ts set.
        3. Verify only the records at or after the threshold arrive, with their strings intact.

        Catalog:
            - DataSubscription

        Since: v3.4.3.6

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-10-23 Added variable-length payload coverage for the filtered path

        """
        self.do_start_ts_varchar()

    def do_start_ts_varchar(self):
        # Trimming the leading rows of a block must also reclaim their variable-length payload,
        # otherwise the column offsets keep pointing into the dropped rows.
        database = "db_start_ts_varchar"
        topic = "topic_start_ts_varchar"
        stb = "stb_start_ts_varchar"
        base_ts = 1640995200000  # 2022-01-01 00:00:00.000 UTC
        rows_total = 20
        first_kept = 10
        threshold_ts = base_ts + first_kept * 1000

        for snapshot in ("false", "true"):
            consumer = None
            try:
                tdSql.execute(f"drop database if exists {database}")
                tdSql.execute(f"create database {database} vgroups 1 wal_retention_period 3600")
                tdSql.execute(f"create stable {database}.{stb} (ts timestamp, c1 varchar(64), c2 bigint) tags(t1 int)")
                tdSql.execute(f"create table {database}.ct using {database}.{stb} tags(1)")
                values = " ".join(
                    "({}, {}, {})".format(base_ts + i * 1000, "NULL" if i % 3 == 0 else f"'v{i:02d}'", i)
                    for i in range(rows_total)
                )
                # one statement, so a single data block spans the start timestamp
                tdSql.execute(f"insert into {database}.ct values {values}")
                if snapshot == "true":
                    tdSql.execute(f"flush database {database}")
                tdSql.execute(f"create topic {topic} as stable {database}.{stb}")

                consumer = Consumer({
                    "group.id": f"g_start_ts_varchar_{snapshot}",
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": "false",
                    "experimental.snapshot.enable": snapshot,
                    "msg.consume.start.ts": str(threshold_ts),
                })
                consumer.subscribe([topic])
                rows = self.consume_rows(consumer, rows_total - first_kept)
                got = sorted((ts_to_ms(row[0]), row[1], row[2]) for row in rows)
                expected = [
                    (base_ts + i * 1000, None if i % 3 == 0 else f"v{i:02d}", i)
                    for i in range(first_kept, rows_total)
                ]
                assert got == expected, f"snapshot={snapshot}: unexpected rows {got}"
                tdLog.info(f"Variable-length payload verified after filtering: snapshot={snapshot}")
            finally:
                if consumer is not None:
                    consumer.close()
                tdSql.execute(f"drop topic if exists {topic}")
                tdSql.execute(f"drop database if exists {database}")

    def test_start_ts_committed_offset(self):
        """A committed offset later than msg.consume.start.ts must win.

        1. Consume a whole topic whose msg.consume.start.ts equals the first record and commit.
        2. Reopen the same group with a lower msg.consume.start.ts.
        3. Verify nothing is re-delivered: the stored position takes precedence.

        Catalog:
            - DataSubscription

        Since: v3.4.3.6

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-10-24 Added committed offset precedence coverage for msg.consume.start.ts

        """
        self.do_start_ts_committed_offset()

    def do_start_ts_committed_offset(self):
        # msg.consume.start.ts is a lower bound for a brand-new group only; a stored offset that is
        # already ahead of it must not be rewound, otherwise every restart would replay data.
        database = "db_start_ts_commit"
        topic = "topic_start_ts_commit"
        stb = "stb_start_ts_commit"
        base_ts = 1640995200000  # 2022-01-01 00:00:00.000 UTC
        rows_total = 20

        for snapshot in ("false", "true"):
            consumer = None
            try:
                tdSql.execute(f"drop database if exists {database}")
                tdSql.execute(f"create database {database} vgroups 1 wal_retention_period 3600")
                tdSql.execute(f"create stable {database}.{stb} (ts timestamp, c1 int) tags(t1 int)")
                tdSql.execute(f"create table {database}.ct using {database}.{stb} tags(1)")
                values = " ".join(f"({base_ts + i * 1000}, {i})" for i in range(rows_total))
                tdSql.execute(f"insert into {database}.ct values {values}")
                if snapshot == "true":
                    tdSql.execute(f"flush database {database}")
                tdSql.execute(f"create topic {topic} as stable {database}.{stb}")

                conf = {
                    "group.id": f"g_start_ts_commit_{snapshot}",
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": "false",
                    "experimental.snapshot.enable": snapshot,
                    "msg.consume.start.ts": str(base_ts),
                }
                consumer = Consumer(conf)
                consumer.subscribe([topic])
                rows = self.consume_rows(consumer, rows_total)
                got = sorted(ts_to_ms(row[0]) for row in rows)
                assert got == [base_ts + i * 1000 for i in range(rows_total)], f"snapshot={snapshot}: {got}"
                consumer.commit()
                consumer.close()
                consumer = None

                # same group, same vgroup, but a threshold before the stored position
                conf["msg.consume.start.ts"] = str(base_ts - 1000)
                consumer = Consumer(conf)
                consumer.subscribe([topic])
                extra = []
                deadline = time.time() + 5
                while time.time() < deadline:
                    result = consumer.poll(1)
                    if not result:
                        continue
                    error = result.error()
                    if error is not None:
                        tdLog.exit(f"TMQ poll failed: {error}")
                    value = result.value()
                    if value is None:  # meta message
                        continue
                    for block in value:
                        extra.extend(block.fetchall())
                if extra:
                    tdLog.exit(
                        f"snapshot={snapshot}: committed offset must win over a lower msg.consume.start.ts, "
                        f"got {len(extra)} re-delivered rows, first {extra[:3]}"
                    )
                tdLog.info(f"Committed offset takes precedence over msg.consume.start.ts: snapshot={snapshot}")
            finally:
                if consumer is not None:
                    consumer.close()
                tdSql.execute(f"drop topic if exists {topic}")
                tdSql.execute(f"drop database if exists {database}")

    def do_start_ts_snapshot_multi_table(self):
        database = "db_start_ts_snap_multi"
        topic = "topic_start_ts_snap_multi"
        stb = "stb_start_ts_snap_multi"
        base_ts = 1640995200000  # 2022-01-01 00:00:00.000 UTC
        rows_per_table = 30
        first_kept = 20
        threshold_ts = base_ts + first_kept * 1000
        consumer = None
        try:
            tdSql.execute(f"drop database if exists {database}")
            tdSql.execute(f"create database {database} vgroups 1 wal_retention_period 3600")
            tdSql.execute(f"create stable {database}.{stb} (ts timestamp, val int) tags(t1 int)")
            for idx in range(3):
                tdSql.execute(f"create table {database}.ct{idx} using {database}.{stb} tags({idx})")

            # ct0/ct1 span the threshold; ct2 lies entirely before it, so a per-table regression
            # there is visible as extra rows instead of a wrong timestamp bound.
            for idx in (0, 1):
                values = " ".join(
                    f"({base_ts + i * 1000}, {i})" for i in range(rows_per_table)
                )
                tdSql.execute(f"insert into {database}.ct{idx} values {values}")
            values = " ".join(
                f"({base_ts - (i + 1) * 1000}, {i})" for i in range(rows_per_table)
            )
            tdSql.execute(f"insert into {database}.ct2 values {values}")

            # Snapshot scan reads the tsdb files, so the rows must be flushed before consuming.
            tdSql.execute(f"flush database {database}")
            tdSql.execute(f"create topic {topic} as database {database}")

            consumer = Consumer({
                "group.id": "g_start_ts_snap_multi",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": "false",
                "experimental.snapshot.enable": "true",
                "msg.consume.start.ts": str(threshold_ts),
            })
            consumer.subscribe([topic])

            expected = 2 * (rows_per_table - first_kept)
            rows = self.consume_rows(consumer, expected)
            for row in rows:
                ms = ts_to_ms(row[0])
                if ms < threshold_ts:
                    tdLog.exit(f"snapshot multi-table: row ts {row[0]} ({ms}) is earlier than start ts {threshold_ts}")
                if row[1] < first_kept:
                    tdLog.exit(f"snapshot multi-table: unexpected filtered value {row[1]} in row {row}")

            # Drain briefly: a broken per-table bound would keep delivering ct2 rows (all of them
            # earlier than the threshold) after the expected rows have arrived.
            deadline = time.time() + 3
            extra = []
            while time.time() < deadline:
                result = consumer.poll(1)
                if not result:
                    continue
                error = result.error()
                if error is not None:
                    tdLog.exit(f"TMQ poll failed: {error}")
                value = result.value()
                if value is None:
                    continue
                for block in value:
                    extra.extend(block.fetchall())
            if extra:
                tdLog.exit(
                    f"snapshot multi-table: {len(extra)} unexpected rows after the filtered set, first {extra[:5]}"
                )
            tdLog.info(f"Multi-table snapshot start ts verified: {len(rows)} rows kept, none before {threshold_ts}")
        finally:
            if consumer is not None:
                consumer.close()
            tdSql.execute(f"drop topic if exists {topic}")
            tdSql.execute(f"drop database if exists {database}")

    def test_filtered_create_table_metadata(self):
        """Preserve metadata when an auto-create INSERT is fully filtered.

        1. Consume a database topic with metadata and a start timestamp.
        2. Verify child-table metadata arrives without filtered data rows.

        Catalog:
            - DataSubscription

        Since: v3.4.3.6

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-10-21 Added filtered metadata regression coverage

        """
        self.do_filtered_create_table_metadata()

    def test_filtered_create_table_metadata_batches(self):
        """Batch filtered auto-create metadata instead of accumulating it.

        1. Consume a database topic with metadata, a start timestamp and min.poll.rows=1.
        2. Replay three auto-create INSERTs whose rows are all filtered out.
        3. Verify every child-table metadata still arrives, in more than one response.

        Catalog:
            - DataSubscription

        Since: v3.4.3.6

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-10-23 Added metadata batching regression for filtered auto-create INSERTs

        """
        self.do_filtered_create_table_metadata_batches()

    def test_invalid_start_ts(self):
        """Reject values that are not a non-negative UNIX timestamp in milliseconds.

        1. Verify a negative integer and a negative epoch in milliseconds fail.
        2. Verify a fractional value and a value with trailing characters fail.
        3. Verify datetime strings and an unparsable string fail.

        Catalog:
            - DataSubscription

        Since: v3.4.3.6

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-10-21 Added negative timestamp regression coverage
            - 2026-10-23 Negative values are rejected instead of accepted
            - 2026-10-24 Only a UNIX timestamp in milliseconds is accepted

        """
        self.do_invalid_start_ts()

    def test_start_ts_precision(self):
        """Convert msg.consume.start.ts to the database precision.

        1. Cover millisecond, microsecond and nanosecond databases.
        2. Verify the inclusive lower bound on both the WAL and the snapshot path.

        Catalog:
            - DataSubscription

        Since: v3.4.3.6

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-10-21 Added precision coverage with pre-epoch data

        """
        self.do_start_ts_precision()

    def test_start_ts_snapshot_multi_table(self):
        """Apply msg.consume.start.ts per table while scanning a multi-table snapshot.

        1. Create a database topic over three child tables of one super table.
        2. Flush so the rows are served by the snapshot scan, then consume with
           experimental.snapshot.enable and a start timestamp.
        3. Verify every child table honors the threshold, and that a child table whose
           rows are all earlier than the threshold delivers nothing at all.

        Catalog:
            - DataSubscription

        Since: v3.4.3.6

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-10-23 Added multi-table snapshot coverage for the per-table lower bound

        """
        self.do_start_ts_snapshot_multi_table()

    def test_tmq_start_ts(self):
        """TMQ msg.consume.start.ts: filter data by primary ts for table/database topics only

        - Table topic (AS STABLE) and database topic (AS DATABASE): only data records
          whose primary timestamp ts is no earlier than msg.consume.start.ts are consumed.
        - Query topic (AS SELECT): the parameter has no effect; filter ts in WHERE instead.
        - The value must be a non-negative UNIX timestamp in milliseconds; datetime strings
          are rejected.

        Catalog:
            - DataSubscription

        Since: v3.4.3.6

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-21 Created

        """
        self.do_tmq_start_ts()

    def do_tmq_start_ts(self):
        database = "db_start_ts"
        stb = "stb_start_ts"
        base_ts = 1640995200000  # 2022-01-01 00:00:00.000 UTC
        rows_total = 100
        threshold_ts = base_ts + 50  # 2022-01-01 00:00:00.050 UTC

        topic_table = "topic_start_ts_table"
        topic_db = "topic_start_ts_db"
        topic_query = "topic_start_ts_query"

        try:
            tdSql.execute(f"drop database if exists {database}")
            tdSql.execute(f"create database {database} vgroups 1 wal_retention_period 3600")
            tdSql.execute(f"create table {database}.{stb} (ts timestamp, c1 int) tags(t1 int)")
            tdSql.execute(f"create table {database}.ct0 using {database}.{stb} tags(0)")
            for i in range(0, rows_total, 10):
                sql = f"insert into {database}.ct0 values "
                for j in range(i, min(i + 10, rows_total)):
                    sql += f"({base_ts + j}, {j}) "
                tdSql.execute(sql)

            tdSql.execute(f"create topic {topic_table} as stable {database}.{stb}")
            tdSql.execute(f"create topic {topic_db} as database {database}")
            tdSql.execute(f"create topic {topic_query} as select * from {database}.{stb}")

            # ---- table topic: rows with ts < threshold must be filtered out ----
            consumer = Consumer({
                "group.id": "g_start_ts_table",
                "td.connect.user": "root",
                "td.connect.pass": "taosdata",
                "enable.auto.commit": "false",
                "auto.offset.reset": "earliest",
                "msg.consume.start.ts": str(threshold_ts),
            })
            try:
                consumer.subscribe([topic_table])
                rows = self.consume_rows(consumer, rows_total - 50)
                for row in rows:
                    if ts_to_ms(row[0]) < threshold_ts:
                        tdLog.exit(f"table topic: row ts {row[0]} is earlier than start ts {threshold_ts}")
            finally:
                consumer.close()

            # ---- database topic: rows with ts < threshold must be filtered out ----
            consumer = Consumer({
                "group.id": "g_start_ts_db",
                "td.connect.user": "root",
                "td.connect.pass": "taosdata",
                "enable.auto.commit": "false",
                "auto.offset.reset": "earliest",
                "msg.consume.start.ts": str(threshold_ts),
            })
            try:
                consumer.subscribe([topic_db])
                rows = self.consume_rows(consumer, rows_total - 50)
                for row in rows:
                    if ts_to_ms(row[0]) < threshold_ts:
                        tdLog.exit(f"database topic: row ts {row[0]} is earlier than start ts {threshold_ts}")
            finally:
                consumer.close()

            # ---- datetime strings are not accepted, only a UNIX timestamp in milliseconds ----
            dt_consumer = None
            try:
                dt_consumer = Consumer({
                    "group.id": "g_start_ts_table_dt",
                    "td.connect.user": "root",
                    "td.connect.pass": "taosdata",
                    "enable.auto.commit": "false",
                    "auto.offset.reset": "earliest",
                    "msg.consume.start.ts": "2022-01-01 00:00:00.050+00:00",
                })
            except TmqError as err:
                tdLog.info(f"datetime string value rejected as expected: {err}")
            else:
                tdLog.exit("msg.consume.start.ts must only accept a UNIX timestamp in milliseconds")
            finally:
                if dt_consumer is not None:
                    dt_consumer.close()

            # ---- query topic: the parameter has no effect, all rows are consumed ----
            consumer = Consumer({
                "group.id": "g_start_ts_query",
                "td.connect.user": "root",
                "td.connect.pass": "taosdata",
                "enable.auto.commit": "false",
                "auto.offset.reset": "earliest",
                "msg.consume.start.ts": str(threshold_ts),
            })
            try:
                consumer.subscribe([topic_query])
                self.consume_rows(consumer, rows_total)
            finally:
                consumer.close()

            tdLog.success(f"{__file__} successfully executed")
        finally:
            tdSql.execute(f"drop topic if exists {topic_table}")
            tdSql.execute(f"drop topic if exists {topic_db}")
            tdSql.execute(f"drop topic if exists {topic_query}")
            tdSql.execute(f"drop database if exists {database}")
