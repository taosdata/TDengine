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

"""Virtual table with an external-only trigger source: restart and registry patch recovery.

The virtual table below maps no local column at all, so every trigger row comes from the
external MySQL table and none of them can be replayed from a local WAL. That makes the three
recovery contracts below observable end to end:

  EXTRC-001  COUNT_WINDOW(3,1): the windows that consumed rows but did not close yet must survive
             a STOP/START. Replaying from the oldest still open window is what rebuilds
             [t2,t4], and the already emitted [t1,t3] must not be produced a second time. The
             next window after recovery ([t3,t5]) must keep its real start and all input rows:
             the recovery filter must not keep trimming new windows with the completed cutoff.
  EXTRC-002  EVENT_WINDOW whose START condition was satisfied by external rows: the open window
             has to be rebuilt from its real start, not from the first row read after the restart.
  EXTRC-003  ALTER VTABLE ... ALTER COLUMN ... SET <same source>.<table>.<other column>: the
             registry identity survives while its only binding is replaced, so the patch has to
             update that reader instead of cleaning it up.
"""

import datetime
import os
import sys
import time

from new_test_framework.utils import tdLog, tdSql, tdStream

_CASE_DIR = os.path.dirname(os.path.abspath(__file__))
_EXT_HELPER_DIR = os.path.abspath(
    os.path.join(
        _CASE_DIR,
        "..",
        "..",
        "05-VirtualTables",
        "01-DDL",
        "03-ExtSource",
    )
)
_FS_DIR = os.path.abspath(os.path.join(_CASE_DIR, "..", "99-Others"))

for helper_dir in (_EXT_HELPER_DIR, _FS_DIR):
    if helper_dir not in sys.path:
        sys.path.insert(0, helper_dir)

from ext_source_helpers import (  # noqa: E402
    ExtSrcEnv,
    create_ext_source,
    create_mysql_table,
    create_remote_db,
)
from fs_common import ensure_snode  # noqa: E402


class TestVirtualStreamExtRecovery:
    DB = "vs_ext_rcv"
    MYSQL_DB = "vs_ext_rcv_mysql"
    MYSQL_SOURCE = "vs_ext_rcv_src"
    REMOTE_TABLE = "same_t"
    VSTB = "rcv_vstb"
    VTABLE = "rcv_vt"
    SITE_TAG = 1

    STREAM_COUNT = "rcv_count_stream"
    SINK_COUNT = "rcv_count_sink"
    STREAM_EVENT = "rcv_event_stream"
    SINK_EVENT = "rcv_event_sink"
    STREAM_PATCH = "rcv_patch_stream"
    SINK_PATCH = "rcv_patch_sink"

    BASE_MS = 1767225600000
    STEP_MS = 1000
    # The external reader samples its history cutoff at the first trigger session, so the fixture
    # seeds one row below the test window and waits for that bootstrap before writing real rows.
    BOOTSTRAP_WAIT_SEC = 40
    # ALTER VTABLE reaches a running stream through the heartbeat patch protocol, which re-deploys
    # the external reader. Wait for that window to close before the next remote row, otherwise the
    # row is read by the previous binding and discarded as stale.
    PATCH_SETTLE_SEC = 20

    @classmethod
    def setup_class(cls):
        tdLog.info(f"start to execute {__file__}")
        cls.env = ExtSrcEnv()
        cls.env.ensure_env()
        ensure_snode()
        tdSql.execute("set timezone 'UTC'")

    @classmethod
    def teardown_class(cls):
        try:
            cls._cleanup_fixture()
        finally:
            cls.env.teardown_env()

    # --- util ---

    @classmethod
    def _cleanup_fixture(cls):
        tdLog.info("cleaning external-only virtual table fixture")
        try:
            for stream in (cls.STREAM_COUNT, cls.STREAM_EVENT, cls.STREAM_PATCH):
                tdSql.execute(f"DROP STREAM IF EXISTS {cls.DB}.{stream}", queryTimes=30)
            tdSql.execute(f"DROP DATABASE IF EXISTS {cls.DB} FORCE")
            tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {cls.MYSQL_SOURCE}")
            tdSql.execute("alter all dnodes 'qdebugflag 143'")
        finally:
            try:
                ExtSrcEnv.mysql_drop_db(cls.MYSQL_DB)
            except Exception as error:
                tdLog.info(f"remote database cleanup skipped: error={error}")

    @classmethod
    def _prepare_fixture(cls):
        cls._cleanup_fixture()
        create_remote_db("mysql", cls.MYSQL_DB)
        create_mysql_table(
            cls.MYSQL_DB,
            cls.REMOTE_TABLE,
            "ts DATETIME(3) NOT NULL PRIMARY KEY, value_a INT, value_b INT",
        )
        # Seed below the test window so the reader's history cutoff stays under the real rows.
        # A naive MySQL DATETIME column is decoded with the connector client time zone, which this
        # case pins to UTC, so every literal written to MySQL must be a UTC rendering.
        seed_text = cls._timestamp(cls.BASE_MS - 2000)
        ExtSrcEnv.mysql_exec(
            cls.MYSQL_DB,
            [f"INSERT INTO {cls.REMOTE_TABLE} VALUES ('{seed_text}', 0, 0)"],
        )
        create_ext_source(cls.MYSQL_SOURCE, "mysql", cls.MYSQL_DB)

        tdSql.execute(f"CREATE DATABASE {cls.DB} VGROUPS 1 PRECISION 'ms'")
        tdSql.execute(
            f"CREATE STABLE {cls.DB}.{cls.VSTB} "
            "(ts TIMESTAMP, remote_a INT) TAGS (site INT) VIRTUAL 1"
        )
        tdSql.execute(
            f"CREATE VTABLE {cls.DB}.{cls.VTABLE} ("
            f"remote_a FROM {cls.MYSQL_SOURCE}.{cls.MYSQL_DB}."
            f"{cls.REMOTE_TABLE}.value_a) "
            f"USING {cls.DB}.{cls.VSTB} TAGS({cls.SITE_TAG})"
        )
        tdSql.execute("alter dnode 1 'qdebugflag 143'")
        tdLog.info("external-only virtual table fixture is ready")

    @classmethod
    def _create_count_stream(cls, stream_name, sink_name, trigger_clause):
        tdSql.execute(
            f"CREATE STREAM {cls.DB}.{stream_name} {trigger_clause} "
            f"FROM {cls.DB}.{cls.VSTB} PARTITION BY tbname "
            "STREAM_OPTIONS(IGNORE_DISORDER) "
            f"INTO {cls.DB}.{sink_name} "
            "(first_ts, last_ts, row_count, remote_sum) AS SELECT "
            "first(_c0), last_row(_c0), count(*), sum(remote_a) FROM %%trows"
        )
        tdStream.checkStreamStatus(stream_name)

    @classmethod
    def _insert_remote_rows(cls, start_index, values_a, compact=False):
        rows = []
        for offset, value_a in enumerate(values_a):
            # MySQL DATETIME(3) is naive: the connector reads the literal in the client time zone
            # (UTC here), so write UTC text. The client renders TIMESTAMP values back in the local
            # time zone, which is what _local_timestamp() produces for the assertions below.
            ts_text = cls._timestamp(
                cls.BASE_MS + (start_index + offset) * cls.STEP_MS
            )
            rows.append(f"('{ts_text}', {value_a}, {value_a + 100})")
        tdLog.info(f"inserting external rows: {rows}")
        if compact:
            ExtSrcEnv.mysql_exec(cls.MYSQL_DB, rows)
        else:
            ExtSrcEnv.mysql_exec(
                cls.MYSQL_DB,
                [f"INSERT INTO {cls.REMOTE_TABLE} VALUES {', '.join(rows)}"],
            )

    @classmethod
    def _stream_status(cls, stream_name):
        try:
            result = tdSql.getResult(
                "SELECT status, message FROM information_schema.ins_streams "
                f"WHERE db_name='{cls.DB}' AND stream_name='{stream_name}'"
            )
            return cls._normalize_rows(result)
        except Exception as error:
            return f"status query failed: {error}"

    @classmethod
    def _wait_bootstrap(cls, stream_name):
        tdLog.info(f"waiting {cls.BOOTSTRAP_WAIT_SEC}s for the external reader bootstrap, stream={stream_name}")
        # Keep the fixed lower bound but poll the stream status on slow CI machines instead of a
        # bare sleep. No bootstrap-completion signal is exposed as a system-table column, so the
        # poll is diagnostic (fails fast in the logs) and the lower bound remains the completion
        # criterion.
        lower_bound = time.time() + cls.BOOTSTRAP_WAIT_SEC
        deadline = lower_bound + 20
        while time.time() < deadline:
            time.sleep(5)
            try:
                status = cls._stream_status(stream_name)
                if status:
                    tdLog.debug(f"bootstrap status poll: {status}")
            except Exception as error:
                tdLog.info(f"bootstrap status poll failed: {error}")
            if time.time() >= lower_bound:
                return
        tdLog.info("bootstrap wait reached its upper bound")

    @classmethod
    def _restart_stream(cls, stream_name):
        tdLog.info(f"restarting stream {stream_name} to exercise checkpoint recovery")
        tdSql.execute(f"STOP STREAM {cls.DB}.{stream_name}")
        tdSql.execute(f"START STREAM {cls.DB}.{stream_name}")
        tdStream.checkStreamStatus(stream_name)

    @classmethod
    def _sink_rows(cls, sink_name, columns):
        result = tdSql.getResult(
            f"SELECT {columns} FROM {cls.DB}.{sink_name} ORDER BY first_ts, last_ts"
        )
        return cls._normalize_rows(result)

    @classmethod
    def _wait_sink_rows(cls, sink_name, columns, expected_rows, timeout=120):
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            try:
                last = cls._sink_rows(sink_name, columns)
                if last == expected_rows:
                    tdLog.info(f"sink rows matched: table={sink_name}, rows={last}")
                    return
            except Exception as error:
                tdLog.info(f"sink rows not ready: table={sink_name}, error={error}")
            time.sleep(0.5)
        raise AssertionError(
            f"sink rows did not match for {sink_name}: got={last}, expected={expected_rows}"
        )

    @classmethod
    def _wait_sink_row_with(cls, sink_name, columns, predicate, timeout=120, label=""):
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            try:
                last = cls._sink_rows(sink_name, columns)
                for row in last:
                    if predicate(row):
                        tdLog.info(f"sink row matched ({label}): table={sink_name}, row={row}")
                        return row
            except Exception as error:
                tdLog.info(f"sink rows not ready: table={sink_name}, error={error}")
            time.sleep(0.5)
        raise AssertionError(
            f"no sink row matched for {sink_name} ({label}): got={last}"
        )

    @classmethod
    def _timestamp(cls, value_ms):
        # UTC rendering: the form to write into a naive remote column while the client zone is UTC.
        return datetime.datetime.fromtimestamp(
            value_ms / 1000, datetime.timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    @classmethod
    def _local_timestamp(cls, value_ms):
        # Local rendering: the form the client returns for a TIMESTAMP column, so use it for the
        # expected sink rows. Do not use it for remote literals.
        return datetime.datetime.fromtimestamp(value_ms / 1000.0).strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )[:-3]

    @classmethod
    def _normalize_rows(cls, rows):
        return [tuple(cls._normalize_cell(cell) for cell in row) for row in rows]

    @classmethod
    def _normalize_cell(cls, value):
        if hasattr(value, "strftime"):
            return value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        if (
            isinstance(value, int)
            and not isinstance(value, bool)
            and abs(value) > 10**15
        ):
            return cls._timestamp(value // 1_000_000)
        return value

    # --- impl ---

    def do_count_window_restart(self):
        self._prepare_fixture()
        try:
            self._create_count_stream(
                self.STREAM_COUNT, self.SINK_COUNT, "COUNT_WINDOW(3,1)"
            )
            self._wait_bootstrap(self.STREAM_COUNT)

            # t1..t3 close [t1,t3]; the windows anchored at t2 and t3 stay open.
            self._insert_remote_rows(0, [1, 2, 3])
            first_ts = self._local_timestamp(self.BASE_MS)
            third_ts = self._local_timestamp(self.BASE_MS + 2 * self.STEP_MS)
            self._wait_sink_rows(
                self.SINK_COUNT,
                "first_ts, last_ts, row_count, remote_sum",
                [(first_ts, third_ts, 3, 6)],
                timeout=90,
            )

            self._restart_stream(self.STREAM_COUNT)

            # t4 completes [t2,t4]. Without replaying the open windows the reader would only fetch
            # rows after t3 and this window could never be produced.
            self._insert_remote_rows(3, [4])
            second_ts = self._local_timestamp(self.BASE_MS + self.STEP_MS)
            fourth_ts = self._local_timestamp(self.BASE_MS + 3 * self.STEP_MS)
            self._wait_sink_rows(
                self.SINK_COUNT,
                "first_ts, last_ts, row_count, remote_sum",
                [
                    (first_ts, third_ts, 3, 6),
                    (second_ts, fourth_ts, 3, 9),
                ],
                timeout=120,
            )

            # t5 closes [t3,t5]. The recovery filter must not keep trimming new windows with the
            # completed cutoff from [t2,t4]: the window would otherwise start at t4+1 and drop
            # the t3 input row (row_count would become 2 and first_ts would move to t4).
            self._insert_remote_rows(4, [5])
            fifth_ts = self._local_timestamp(self.BASE_MS + 4 * self.STEP_MS)
            self._wait_sink_rows(
                self.SINK_COUNT,
                "first_ts, last_ts, row_count, remote_sum",
                [
                    (first_ts, third_ts, 3, 6),
                    (second_ts, fourth_ts, 3, 9),
                    (third_ts, fifth_ts, 3, 12),
                ],
                timeout=120,
            )
            tdLog.info("count window restart ......................... [ passed ]")
        finally:
            self._cleanup_fixture()

    def do_event_window_restart(self):
        self._prepare_fixture()
        try:
            self._create_count_stream(
                self.STREAM_EVENT,
                self.SINK_EVENT,
                "EVENT_WINDOW(START WITH remote_a > 0 END WITH remote_a < 0)",
            )
            self._wait_bootstrap(self.STREAM_EVENT)

            # The start condition is satisfied by external rows only, so the open window lives in
            # the trigger state and not in any replayable local WAL.
            self._insert_remote_rows(0, [1, 2])
            first_ts = self._local_timestamp(self.BASE_MS)
            third_ts = self._local_timestamp(self.BASE_MS + 2 * self.STEP_MS)

            self._restart_stream(self.STREAM_EVENT)

            # The end condition closes the window, which must still start at its real first row.
            self._insert_remote_rows(2, [-3])
            row = self._wait_sink_row_with(
                self.SINK_EVENT,
                "first_ts, last_ts, row_count, remote_sum",
                lambda candidate: candidate[1] == third_ts,
                timeout=120,
                label="closed event window",
            )
            if row[0] != first_ts:
                raise AssertionError(
                    f"event window restarted at {row[0]} instead of its real start {first_ts}"
                )
            tdLog.info(f"event window closed over rows: {row}")
            tdLog.info("event window restart ......................... [ passed ]")
        finally:
            self._cleanup_fixture()

    def do_alter_column_same_registry(self):
        self._prepare_fixture()
        try:
            self._create_count_stream(
                self.STREAM_PATCH, self.SINK_PATCH, "COUNT_WINDOW(1)"
            )
            self._wait_bootstrap(self.STREAM_PATCH)

            self._insert_remote_rows(0, [11])
            first_ts = self._local_timestamp(self.BASE_MS)
            self._wait_sink_rows(
                self.SINK_PATCH,
                "first_ts, last_ts, row_count, remote_sum",
                [(first_ts, first_ts, 1, 11)],
                timeout=90,
            )

            # Same source, same database, same table, only the referenced column changes: the
            # registry identity survives, so this must update the reader instead of cleaning it up.
            tdSql.execute(
                f"ALTER VTABLE {self.DB}.{self.VTABLE} ALTER COLUMN remote_a SET "
                f"{self.MYSQL_SOURCE}.{self.MYSQL_DB}.{self.REMOTE_TABLE}.value_b"
            )
            tdStream.checkStreamStatus(self.STREAM_PATCH)
            tdLog.info(f"waiting {self.PATCH_SETTLE_SEC}s for the external registry patch to settle")
            # Poll the stream status for the settle window (plus a bounded grace period on slow
            # machines) instead of a bare fixed sleep: a heartbeat patch that has not landed yet
            # would read the next row through the previous binding and drop it as stale.
            settle_lower = time.time() + self.PATCH_SETTLE_SEC
            settle_deadline = settle_lower + 10
            while time.time() < settle_deadline:
                time.sleep(2)
                try:
                    tdStream.checkStreamStatus(self.STREAM_PATCH)
                except Exception as error:
                    tdLog.info(f"patch settle status poll failed: {error}")
                if time.time() >= settle_lower:
                    break

            self._insert_remote_rows(1, [12])
            second_ts = self._local_timestamp(self.BASE_MS + self.STEP_MS)
            self._wait_sink_rows(
                self.SINK_PATCH,
                "first_ts, last_ts, row_count, remote_sum",
                [
                    (first_ts, first_ts, 1, 11),
                    (second_ts, second_ts, 1, 112),
                ],
                timeout=90,
            )
            tdLog.info("alter column same registry ................... [ passed ]")
        finally:
            self._cleanup_fixture()

    # --- main ---

    def test_virtual_stream_ext_count_window_restart(self):
        """COUNT_WINDOW(3,1) over an external-only virtual table survives a STOP/START

        1. Close one count window from external rows and restart the stream
        2. The windows that were consumed but not closed are rebuilt and the already emitted
           window is not produced again

        Catalog:
            - Stream

        Since: v3.4.3.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7034326784

        Jira: None

        History:
            - 2026-09-27 mmwang New case for the external replay floor

        """
        self.do_count_window_restart()

    def test_virtual_stream_ext_event_window_restart(self):
        """EVENT_WINDOW started by external rows survives a STOP/START

        1. Satisfy the start condition with external rows and restart the stream
        2. The end condition closes a window that still starts at its real first row

        Catalog:
            - Stream

        Since: v3.4.3.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7034326784

        Jira: None

        History:
            - 2026-09-27 mmwang New case for the EVENT tree replay floor

        """
        self.do_event_window_restart()

    def test_virtual_stream_ext_alter_column_same_registry(self):
        """ALTER VTABLE ... SET <same source>.<table>.<other column> keeps the stream running

        1. Remap the only external binding of a virtual table to another column of the same table
        2. The stream stays healthy and reads the new column

        Catalog:
            - Stream

        Since: v3.4.3.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7034326784

        Jira: None

        History:
            - 2026-09-27 mmwang New case for the sole owner registry patch

        """
        self.do_alter_column_same_registry()
