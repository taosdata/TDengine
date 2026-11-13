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

"""Oracle TIMESTAMP(9) streams must preserve source nanosecond precision.

The synthetic LAST_TS and MIN/MAX column mappings used to carry only
``tdType.precision`` and left ``tsPrecision`` zero; the Oracle connector reads
``tsPrecision`` and defaults every non-NANO value to microseconds, so a
TIMESTAMP(9) row whose timestamp differs only below one microsecond was
truncated in META.  The DATA fetch then bounds ``ts <= ekey`` with the
truncated value and excludes the real row, leaving the trigger stuck
re-discovering a row it can never fetch.  A virtual-table projection has the
same requirement for non-key timestamp columns.
"""

import datetime
import sys
import time

from new_test_framework.utils import tdLog, tdSql, tdStream

sys.path.insert(0, "cases/09-DataQuerying/19-FederatedQuery")
sys.path.insert(0, "cases/18-StreamProcessing/99-Others")

from federated_query_common import ExtSrcEnv, FederatedQueryTestMixin  # noqa: E402
from fs_common import ensure_snode  # noqa: E402


class TestFsOracleTs9Trigger(FederatedQueryTestMixin):
    DB = "fs_ts9"
    STREAM = "s_ts9"
    SINK = "sink"
    TABLE = "src_t9"
    SCHEMA = ExtSrcEnv.ORACLE_USER.upper()

    @classmethod
    def setup_class(cls):
        cls.env = ExtSrcEnv()
        cls.env.ensure_env()
        ensure_snode()
        tdSql.execute(f"DROP DATABASE IF EXISTS {cls.DB}")
        tdSql.execute(f"CREATE DATABASE {cls.DB} VGROUPS 1 PRECISION 'ns'")
        tdSql.execute("set timezone 'UTC'")

    @classmethod
    def teardown_class(cls):
        try:
            tdSql.execute(f"DROP DATABASE IF EXISTS {cls.DB}")
        finally:
            cls.env.teardown_env()

    @staticmethod
    def _literal(ts_ns):
        """Render an epoch-nanosecond instant as an Oracle TIMESTAMP literal.

        All nine fraction digits are emitted (11g rejects the ISO-8601 'T'
        separator, so the space-separated form is used for every generation);
        ``datetime`` only carries microseconds, hence the manual fraction.
        """
        seconds, frac_ns = divmod(ts_ns, 1000000000)
        stamp = datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=seconds)
        return f"{stamp.strftime('%Y-%m-%d %H:%M:%S')}.{frac_ns:09d}"

    def _wait_sink_value(self, ts_ns, expected, timeout=30, column="val",
                         ts_column="ts"):
        """Wait until the sink row at ``ts_ns`` carries ``expected`` in ``column``."""
        deadline = time.monotonic() + timeout
        last_error = None
        while time.monotonic() < deadline:
            try:
                res = tdSql.getResult(
                    f"SELECT {column} FROM {self.DB}.{self.SINK} "
                    f"WHERE {ts_column} = {ts_ns}")
            except Exception as error:  # the sink table may not exist yet
                last_error = error
                time.sleep(0.5)
                continue
            if res and [tuple(row) for row in res] == [(expected,)]:
                return True
            time.sleep(0.5)
        tdLog.debug(
            f"sink row ts={ts_ns} was not observed as {column}={expected}: {last_error}")
        return False

    def test_oracle_timestamp9_trigger(self):
        """One TIMESTAMP(9) row with sub-microsecond digits must reach the sink.

        1. Create a TIMESTAMP(9) table in the Oracle login schema and a direct
           external-trigger stream over it.
        2. Prove the reader already sampled its history watermark by observing a
           canary row in the sink.
        3. Insert one row whose timestamp differs from a whole microsecond only
           in its last three digits, then check that a window closes and the
           value lands in the sink.

        The row's timestamp differs from a whole microsecond only in the last
        three digits; if LAST_TS / MIN / MAX decode it at microseconds, the
        META ekey truncates to ``...123456000`` and the DATA ``ts <= ekey``
        bound excludes ``...123456789``, so no window ever closes.

        Catalog:
            - Stream

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7132783339

        History:
            - 2026-11-02 Wang Mingming Add Oracle TIMESTAMP(9) trigger coverage.
            - 2026-11-05 Wang Mingming Wait for the history watermark before the
              measured insert, so the row cannot be consumed as history.
        """
        prefix = "ts9"
        # 2026-01-01T00:00:00Z in nanoseconds; the measured row is 123456789 ns
        # later, i.e. inside the second the canaries below precede.
        base_ns = 1767225600000000000
        tested_ns = base_ns + 123456789
        oracle_setup = [
            # A re-run without an environment reset still has the table from the
            # previous attempt, and CREATE TABLE would then fail with ORA-00955.
            f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE \"{self.TABLE}\"'; "
            f"EXCEPTION WHEN OTHERS THEN NULL; END;",
            f'CREATE TABLE "{self.TABLE}" ("ts" TIMESTAMP(9) PRIMARY KEY, "val" NUMBER(10))',
        ]

        def body(src_name, db_type):
            stream = f"{self.DB}.{self.STREAM}"
            sink = f"{self.DB}.{self.SINK}"
            ref = f"{src_name}.`{self.SCHEMA}`.{self.TABLE}"
            tdSql.execute(f"USE {self.DB}")
            tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
            tdSql.execute(f"DROP TABLE IF EXISTS {sink}")
            tdSql.execute(
                f"CREATE STREAM {stream} COUNT_WINDOW(1) FROM {ref} "
                f"INTO {sink} AS SELECT ts, val FROM {ref}"
            )
            tdStream.checkStreamStatus(self.STREAM)

            # checkStreamStatus only waits for Trigger=Running, which the engine
            # sets when it queues the START request; the reader's first
            # LAST_TS / MAX(ts) sample can still be pending.  A row written before
            # that sample becomes the history cutoff and is never emitted, so keep
            # writing canaries below the measured instant until one is observed in
            # the sink: that proves the sample already happened.
            ready = False
            for attempt in range(10):
                canary_ns = base_ns - (10 - attempt) * 1000000000
                ExtSrcEnv.oracle_exec_cfg(
                    self._oracle_cfg(),
                    [f'INSERT INTO "{self.TABLE}" VALUES '
                     f"(TIMESTAMP '{self._literal(canary_ns)}', 0)", "COMMIT"],
                )
                if self._wait_sink_value(canary_ns, 0, timeout=20):
                    ready = True
                    break
            assert ready, (
                "the external reader never emitted a canary row, so its history "
                "watermark was not sampled and the measured row would be dropped"
            )

            ExtSrcEnv.oracle_exec_cfg(
                self._oracle_cfg(),
                [f'INSERT INTO "{self.TABLE}" VALUES '
                 f"(TIMESTAMP '{self._literal(tested_ns)}', 1)", "COMMIT"],
            )
            assert self._wait_sink_value(tested_ns, 1, timeout=120), (
                f"no sink row for {self._literal(tested_ns)}: the sub-microsecond "
                f"digits were truncated by the trigger watermark"
            )
            res = tdSql.getResult(f"SELECT val FROM {sink} WHERE ts = {tested_ns}")
            tdSql.checkEqual(res, [(1,)])
            tdSql.execute(f"DROP STREAM {stream}")
            tdSql.execute(f"DROP TABLE IF EXISTS {sink}")

        self._with_custom_sources(
            prefix, body, oracle_setup=oracle_setup,
            skip_mysql=True, skip_pg=True, skip_influx=True, skip_sqlserver=True,
        )

    def test_oracle_vtable_side_timestamp9_projection(self):
        """A non-key TIMESTAMP(9) column must keep its sub-microsecond digits.

        1. Create an Oracle table whose primary key is TIMESTAMP(9) and that
           also holds a side ``EVENT_TS TIMESTAMP(9)`` column.
        2. Bind both through a nanosecond virtual table and start a stream that
           projects ``event_ts``.
        3. Insert a row whose ``event_ts`` carries digits below one microsecond
           and check the sink holds the exact nanosecond value.

        The reader decodes every timestamp column at the table's transport
        precision, not only the primary-key one.  Carrying it only on the
        timestamp mapping left ``tsPrecision`` zero on the side column, and the
        Oracle connector reads every non-NANO mapping as microseconds: the last
        three digits are gone at fetch time, and normalizing the block to the
        nanosecond stream precision can only pad zeros.

        Catalog:
            - Stream

        Since: v3.4.4.0

        Labels: common,ci

        Jira: None

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7132783339

        History:
            - 2026-11-05 Wang Mingming Cover non-key TIMESTAMP(9) projection
              through a nanosecond virtual table.
        """
        table = "src_side_t9"
        vtable = "vt_side_t9"
        vstable = "vstb_side_t9"
        # 2026-01-01T00:00:00Z in nanoseconds.  The measured row sits at
        # ...123456789 and projects a side timestamp at ...987654321, so both
        # values differ from a whole microsecond only in their last three digits.
        base_ns = 1767225600000000000
        tested_ns = base_ns + 123456789
        side_ns = base_ns + 987654321
        oracle_setup = [
            # An explicit NUMBER(10,0) keeps the column unambiguously BIGINT, which is
            # what the virtual table below declares.
            f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE \"{table}\"'; "
            f"EXCEPTION WHEN OTHERS THEN NULL; END;",
            f'CREATE TABLE "{table}" ("ts" TIMESTAMP(9) PRIMARY KEY, '
            f'"event_ts" TIMESTAMP(9), "val" NUMBER(10,0))',
        ]

        def body(src_name, db_type):
            stream = f"{self.DB}.{self.STREAM}"
            sink = f"{self.DB}.{self.SINK}"
            remote = f"{src_name}.`{self.SCHEMA}`.{table}"
            vt = f"{self.DB}.{vtable}"
            vstb = f"{self.DB}.{vstable}"
            tdSql.execute(f"USE {self.DB}")
            tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
            tdSql.execute(f"DROP TABLE IF EXISTS {sink}")
            tdSql.execute(f"DROP VTABLE IF EXISTS {vt}")
            tdSql.execute(f"DROP STABLE IF EXISTS {vstb}")
            # A stream needs the virtual stable: it supplies the timestamp axis and
            # the tag set, while the vtable binds the remote columns.  A bare vtable
            # cannot back a stream (the trigger resolves the external bindings from
            # the stable the vtable belongs to).
            tdSql.execute(
                f"CREATE STABLE {vstb} (ts TIMESTAMP, event_ts TIMESTAMP, "
                f"val BIGINT) TAGS (site INT) VIRTUAL 1"
            )
            tdSql.execute(
                f"CREATE VTABLE {vt} ("
                f"event_ts FROM {remote}.event_ts, "
                f"val FROM {remote}.val) USING {vstb} TAGS(1)"
            )
            tdSql.execute(
                f"CREATE STREAM {stream} COUNT_WINDOW(1) FROM {vstb} "
                f"PARTITION BY tbname STREAM_OPTIONS(IGNORE_DISORDER) "
                f"INTO {sink} (win_ts, event_ts, val, row_count) "
                f"AS SELECT first(_c0), max(event_ts), sum(val), count(*) "
                f"FROM %%trows"
            )
            tdStream.checkStreamStatus(self.STREAM)

            # Same readiness proof as above: a row written before the reader
            # samples its history watermark is consumed as history and never
            # emitted, so keep writing canaries until one reaches the sink.
            ready = False
            for attempt in range(10):
                canary_ns = base_ns - (10 - attempt) * 1000000000
                ExtSrcEnv.oracle_exec_cfg(
                    self._oracle_cfg(),
                    [f'INSERT INTO "{table}" VALUES '
                     f"(TIMESTAMP '{self._literal(canary_ns)}', "
                     f"TIMESTAMP '{self._literal(canary_ns)}', 0)", "COMMIT"],
                )
                if self._wait_sink_value(canary_ns, 0, timeout=20,
                                         ts_column="win_ts"):
                    ready = True
                    break
            assert ready, (
                "the external reader never emitted a canary row, so its history "
                "watermark was not sampled and the measured row would be dropped"
            )

            ExtSrcEnv.oracle_exec_cfg(
                self._oracle_cfg(),
                [f'INSERT INTO "{table}" VALUES '
                 f"(TIMESTAMP '{self._literal(tested_ns)}', "
                 f"TIMESTAMP '{self._literal(side_ns)}', 7)", "COMMIT"],
            )
            assert self._wait_sink_value(tested_ns, 7, timeout=120,
                                         ts_column="win_ts"), (
                f"no sink row for {self._literal(tested_ns)}"
            )
            res = tdSql.getResult(
                f"SELECT event_ts FROM {sink} WHERE win_ts = {tested_ns}")
            tdSql.checkEqual(res, [(side_ns,)])
            tdSql.execute(f"DROP STREAM {stream}")
            tdSql.execute(f"DROP TABLE IF EXISTS {sink}")
            tdSql.execute(f"DROP VTABLE IF EXISTS {vt}")
            tdSql.execute(f"DROP STABLE IF EXISTS {vstb}")

        self._with_custom_sources(
            "ts9side", body, oracle_setup=oracle_setup,
            skip_mysql=True, skip_pg=True, skip_influx=True, skip_sqlserver=True,
        )
