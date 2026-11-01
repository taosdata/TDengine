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

"""FS §9 — ext calc fetch bounds for time-returning window expressions.

The external-table reader (ETR) derives its remote-fetch time range the same
way the vnode reader does (vnodeStream.c ``processCalaTimeRange``): when the
calc SELECT carries time-returning expressions (``STimeRangeNode.needCalc``,
e.g. ``_twstart - offset`` or ``cast(_tprev_localtime / d as timestamp)``) the
bounds are evaluated against the current window parameters instead of trusting
the raw merged ``[wstart, wend]`` range.

Cases:
  FS-BND-001  INTERVAL + shifted window bounds (``>= _twstart - 30s`` /
              ``< _twend - 30s``): every row lands in exactly one window.
  FS-BND-002  INTERVAL + mixed upper bound (``>= _twstart`` /
              ``<= _twstart + 30s``): the inclusive ``<=`` bound keeps the
              boundary row.
  FS-BND-003  PERIOD + casted localtime bounds: rows written near "now" are
              captured exactly once (no row dropped by a mis-scaled bound).
"""

import sys
import time

import pytest

from new_test_framework.utils import tdLog, tdSql, tdStream

sys.path.insert(0, "cases/09-DataQuerying/19-FederatedQuery")
from federated_query_common import (  # noqa: E402
    ExtSrcEnv,
    FederatedQueryTestMixin,
)

sys.path.insert(0, "cases/18-StreamProcessing/99-Others")
from fs_common import (  # noqa: E402
    ensure_snode,
    wait_stream_window_closed,
    verify_sink_rows,
    FS_BASE_MS,
    ms_to_dt,
    dt_str,
    FsSharedFixtureMixin,
)


def _mid_segment(prefix: str, src_name: str) -> str:
    """For src '{prefix}_{m|p|s|i}', return remote db (mysql/influx) or schema (pg/sqlserver)."""
    suffix = src_name[-1]
    if suffix == "p":
        return "public"
    if suffix == "s":
        # SQL Server 3-segment refs carry the schema in the middle segment;
        # the database comes from the source default (DS §5.3.3).
        return "dbo"
    return f"{prefix}_{suffix}db"


def _src_divisor(src_name: str) -> int:
    """_twstart/_twend are in the external ts-column precision:
    microseconds for MySQL/PG, nanoseconds for InfluxDB."""
    return 1_000_000 if src_name.endswith("_i") else 1_000


def _src_ts_col(src_name: str) -> str:
    return "time" if src_name.endswith("_i") else "ts"


class TestFsExtWindowBounds(FsSharedFixtureMixin, FederatedQueryTestMixin):
    """FS §9 — external-calc fetch bounds driven by time-returning expressions."""

    DB = "fs_extbnd"

    # Sink database for FS-BND-003: the derived-table wrapper loses the inner
    # cast's column precision, so the sink table is auto-created with the
    # database's default precision and a mismatched ts value is rejected as
    # "Timestamp data out of range".  MySQL/PG casts produce microsecond
    # values; InfluxDB casts produce nanosecond values.
    # TODO: the precision loss is a separate planner bug; once it
    # is fixed, write FS-BND-003 into cls.DB and drop these databases.
    US_DB = "fs_extbnd_us"
    NS_DB = "fs_extbnd_ns"

    _BASE_MS = FS_BASE_MS

    _ms_to_dt = staticmethod(ms_to_dt)
    _dt_str = staticmethod(dt_str)

    @classmethod
    def setup_class(cls):
        cls.env = ExtSrcEnv()
        cls.env.ensure_env()
        ensure_snode()
        tdSql.execute(f"DROP DATABASE IF EXISTS {cls.DB}")
        tdSql.execute(f"CREATE DATABASE {cls.DB} PRECISION 'ms'")
        tdSql.execute(f"DROP DATABASE IF EXISTS {cls.US_DB}")
        tdSql.execute(f"CREATE DATABASE {cls.US_DB} PRECISION 'us'")
        tdSql.execute(f"DROP DATABASE IF EXISTS {cls.NS_DB}")
        tdSql.execute(f"CREATE DATABASE {cls.NS_DB} PRECISION 'ns'")

    @classmethod
    def teardown_class(cls):
        try:
            tdSql.execute(f"DROP DATABASE IF EXISTS {cls.DB}")
            tdSql.execute(f"DROP DATABASE IF EXISTS {cls.US_DB}")
            tdSql.execute(f"DROP DATABASE IF EXISTS {cls.NS_DB}")
        finally:
            cls.env.teardown_env()

    # ------------------------------------------------------------------
    # FS-BND-001  INTERVAL + shifted window bounds.
    #
    # windows are 1m wide; the calc WHERE shifts both bounds back by 30s:
    #   [wstart - 30s, wend - 30s).  Row timestamps (window-relative):
    #   08:00:00 -> w08:00   08:01:00 / 08:01:20 -> w08:01
    #   08:02:00 / 08:02:20 -> w08:02    08:03:00 -> w08:03 (open)
    # ------------------------------------------------------------------
    def test_bnd_001_interval_windowed_time_range_offset(self):
        """INTERVAL stream over an ext table with a shifted _twstart/_twend range.

        The calc SELECT bounds are ``ts >= _twstart - 30s AND ts < _twend - 30s``
        (``needCalc=true``).  Each source row must be aggregated in exactly one
        window: an off-by-one in the evaluated bounds would drop or duplicate
        the 08:01:20 / 08:02:20 rows that sit near the shifted boundary.

        Catalog:
            - Streams:Federated:ExtWindowBounds

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7106459789

        History:
            - 2026-09-29 mmwang Created
        """
        prefix = "bnd001"
        offset_ms = 30_000

        rows = [
            (self._BASE_MS,            6,  6.0, 'a', 0),
            (self._BASE_MS + 60_000,   7,  7.0, 'b', 0),
            (self._BASE_MS + 80_000,   8,  8.0, 'c', 0),
            (self._BASE_MS + 120_000,  9,  9.0, 'd', 0),
            (self._BASE_MS + 140_000, 10, 10.0, 'e', 0),
            (self._BASE_MS + 180_000, 11, 11.0, 'f', 0),
        ]

        EXPECTED = [
            (self._ms_to_dt(self._BASE_MS),          1, 6.0),
            (self._ms_to_dt(self._BASE_MS + 60_000), 2, 7.5),
            (self._ms_to_dt(self._BASE_MS + 120_000), 2, 9.5),
        ]

        def body(src_name: str):
            mid = _mid_segment(prefix, src_name)
            suffix = src_name[-1]
            db_or_bucket = f"{prefix}_{suffix}db"
            stream = f"s_{src_name}"
            sink = f"sink_{src_name}"
            div = _src_divisor(src_name)
            ts_col = _src_ts_col(src_name)
            offset = offset_ms * div

            tdSql.execute(f"USE {self.DB}")
            tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")
            tdSql.execute(
                f"CREATE STREAM {stream} INTERVAL(1m) SLIDING(1m) "
                f"FROM {src_name}.{mid}.src_t "
                f"INTO {self.DB}.{sink} AS "
                f"SELECT cast(_twstart/{div} as timestamp) AS ts, "
                f"COUNT(*) AS cnt, AVG(val) AS avg_val "
                f"FROM {src_name}.{mid}.src_t "
                f"WHERE {ts_col} >= _twstart - {offset} "
                f"AND {ts_col} < _twend - {offset}"
            )
            tdStream.checkStreamStatus(stream)

            self._insert_rows(suffix, db_or_bucket, rows, src_name)
            verify_sink_rows(self.DB, stream, sink, EXPECTED, src_name)

            tdSql.execute(f"DROP STREAM IF EXISTS {stream}", queryTimes=30)
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")

        # Streams reject Oracle external tables (streamTriggerTask's identity
        # check is fail-closed for types past SQL Server), so the stream
        # matrices never provision an Oracle leg.
        self._with_std_sources(prefix, body, skip_oracle=True)

    # ------------------------------------------------------------------
    # FS-BND-002  INTERVAL + mixed upper bound.
    #
    #   [wstart, wstart + 30s] via ``>= _twstart AND <= _twstart + 30s``.
    #   ``<=`` must include the boundary row at 08:00:30 (needCalc=true,
    #   LOWER_EQUAL -> ekey + 1 -> inclusive after the fetch conversion).
    # ------------------------------------------------------------------
    def test_bnd_002_interval_windowed_time_range_mixed(self):
        """INTERVAL stream with an inclusive upper bound on the ext scan.

        The calc SELECT bounds are ``ts >= _twstart AND ts <= _twstart + 30s``
        (``needCalc=true``).  The row exactly at ``wstart + 30s`` must be kept:
        dropping it would break the inclusive ``<=`` semantics of the range.

        Catalog:
            - Streams:Federated:ExtWindowBounds

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7106459789

        History:
            - 2026-09-29 mmwang Created
        """
        prefix = "bnd002"
        offset_ms = 30_000

        rows = [
            (self._BASE_MS,            6, 6.0, 'a', 0),
            (self._BASE_MS + 30_000,   7, 7.0, 'b', 0),
            (self._BASE_MS + 60_000,   8, 8.0, 'c', 0),
            (self._BASE_MS + 80_000,   9, 9.0, 'd', 0),
            (self._BASE_MS + 120_000, 10, 10.0, 'e', 0),
        ]

        EXPECTED = [
            (self._ms_to_dt(self._BASE_MS),         2, 6.5),
            (self._ms_to_dt(self._BASE_MS + 60_000), 2, 8.5),
        ]

        def body(src_name: str):
            mid = _mid_segment(prefix, src_name)
            suffix = src_name[-1]
            db_or_bucket = f"{prefix}_{suffix}db"
            stream = f"s_{src_name}"
            sink = f"sink_{src_name}"
            div = _src_divisor(src_name)
            ts_col = _src_ts_col(src_name)
            offset = offset_ms * div

            tdSql.execute(f"USE {self.DB}")
            tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")
            tdSql.execute(
                f"CREATE STREAM {stream} INTERVAL(1m) SLIDING(1m) "
                f"FROM {src_name}.{mid}.src_t "
                f"INTO {self.DB}.{sink} AS "
                f"SELECT cast(_twstart/{div} as timestamp) AS ts, "
                f"COUNT(*) AS cnt, AVG(val) AS avg_val "
                f"FROM {src_name}.{mid}.src_t "
                f"WHERE {ts_col} >= _twstart "
                f"AND {ts_col} <= _twstart + {offset}"
            )
            tdStream.checkStreamStatus(stream)

            self._insert_rows(suffix, db_or_bucket, rows, src_name)
            verify_sink_rows(self.DB, stream, sink, EXPECTED, src_name)

            tdSql.execute(f"DROP STREAM IF EXISTS {stream}", queryTimes=30)
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")

        # Streams reject Oracle external tables (streamTriggerTask's identity
        # check is fail-closed for types past SQL Server), so the stream
        # matrices never provision an Oracle leg.
        self._with_std_sources(prefix, body, skip_oracle=True)

    # ------------------------------------------------------------------
    # FS-BND-003  PERIOD + casted localtime bounds.
    #
    #   ``ts >= cast(_tprev_localtime / d as timestamp)
    #    AND ts <  cast(_tlocaltime / d as timestamp)``
    #   _t*_localtime are ns; d=1e3 maps them to the MySQL/PG microsecond
    #   ts column (d=1 for InfluxDB nanoseconds).  Adjacent period windows
    #   share the instant ``_tlocaltime(N) == _tprev_localtime(N+1)``, so the
    #   bounds must be half-open (same convention as FS-BND-001).  With a
    #   closed upper bound a row whose ts lands exactly on that instant is
    #   fetched by both windows and counted twice.
    # ------------------------------------------------------------------
    _BND003_BATCHES = 5
    _BND003_BATCH_ROWS = 3

    # InfluxDB 3 only makes a write queryable after its next WAL flush.  A
    # write-to-visible delay longer than the 1s period window means every batch
    # is flushed after the window that should have aggregated it has already
    # run, so the sink can stay empty on an environment limitation alone.  This
    # threshold gates an explicit pytest.skip; a faster probe means the empty
    # sink is a real regression and the assertion must fire.
    _BND003_INFLUX_MAX_VISIBLE_DELAY_S = 1.0

    def _influx_visible_delay(self, bucket: str, timeout_s: float = 10.0):
        """Seconds between an InfluxDB write and its first visible read.

        Probes the InfluxDB instance directly (never through the TDengine
        federated path) with a throwaway measurement, so a stream/reader
        regression cannot be mistaken for WAL-flush latency.  Returns None
        when the probe point never became queryable within ``timeout_s``.

        The probe writes into the per-run bucket created by
        ``_with_std_sources`` for this case, which that helper drops right
        after ``body_fn`` returns, so the ``visprobe`` measurement leaves no
        residue behind.
        """
        ts_ns = int(time.time() * 1_000_000_000)
        tag = f"probe{ts_ns}"
        line = f'visprobe tag="{tag}",val=1i {ts_ns}'

        start = time.time()
        try:
            ExtSrcEnv.influx_write_cfg(self._influx_cfg(), bucket, [line])
        except Exception as e:
            tdLog.info(f"[visprobe] InfluxDB probe write failed: {e}")
            return None

        deadline = start + timeout_s
        while time.time() < deadline:
            try:
                res = ExtSrcEnv.influx_query_sql_cfg(
                    self._influx_cfg(), bucket,
                    f"select count(*) as total from visprobe where tag = '{tag}'")
            except Exception as e:
                tdLog.info(f"[visprobe] InfluxDB probe query failed: {e}")
                res = None
            if res and isinstance(res[0], dict) and res[0].get("total"):
                return time.time() - start
            time.sleep(0.2)
        return None

    @staticmethod
    def _bnd003_sink_sum(sink_db: str, sink: str):
        """(window rows, SUM(cnt)) of the sink; (0, 0) before it exists."""
        try:
            tdSql.query(f"SELECT COUNT(*), SUM(cnt) FROM {sink_db}.{sink}",
                        queryTimes=1)
        except Exception:
            return 0, 0
        if not tdSql.queryResult:
            return 0, 0
        return (tdSql.queryResult[0][0] or 0), (tdSql.queryResult[0][1] or 0)

    @classmethod
    def _bnd003_wait_stable(cls, sink_db: str, sink: str, stable_s: float,
                            timeout_s: float):
        """Wait until SUM(cnt) is unchanged for ``stable_s`` seconds."""
        deadline = time.time() + timeout_s
        last = cls._bnd003_sink_sum(sink_db, sink)
        since = time.time()
        while time.time() < deadline:
            time.sleep(0.5)
            cur = cls._bnd003_sink_sum(sink_db, sink)
            if cur != last:
                last, since = cur, time.time()
            elif time.time() - since >= stable_s:
                return last
        return last

    def test_bnd_003_period_casted_localtime_bounds(self):
        """PERIOD stream bounded by casted localtime placeholders.

        The calc SELECT bounds use ``cast(_tprev_localtime / d as timestamp)``
        and ``cast(_tlocaltime / d as timestamp)`` (``needCalc=true``).  A
        mis-scaled remote bound (e.g. the raw trigger-precision union values)
        would filter the freshly written rows out, and an off-by-one bound
        would drop or double-count rows near a period boundary.  After the
        trigger is warm, every row written must be counted exactly once:
        SUM(cnt) over the new windows equals the number of rows written.

        Adjacent period windows share the instant ``_tlocaltime(N) ==
        _tprev_localtime(N+1)``, so the lower bound is inclusive and the upper
        bound is exclusive (``ts >= prev AND ts < cur``), the same half-open
        convention as FS-BND-001.  The last measured batch is pinned so its
        newest row sits exactly on a whole second: that row must still be
        counted once, which a closed upper bound would violate.

        InfluxDB 3 only makes a write queryable after its next WAL flush
        (``--wal-flush-interval``, default 1s), which can land after the
        period window closed and its calc already ran.  Such a batch is
        legitimately missed by the localtime snapshot, so for InfluxDB only
        "counted at most once" is asserted; the warm-up window still proves
        the bounds are correctly scaled.  If no window aggregates at all, the
        InfluxDB write-to-visible delay is probed directly: only a delay
        beyond the 1s window triggers an explicit skip, an empty sink on a
        healthy source fails the test.

        Catalog:
            - Streams:Federated:ExtWindowBounds

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7106459789

        History:
            - 2026-09-29 mmwang Created
        """
        prefix = "bnd003"
        n_batches = self._BND003_BATCHES
        batch_rows = self._BND003_BATCH_ROWS
        # Set only when the InfluxDB leg hits an environment limitation; the
        # remaining sources still run so their assertions keep their value.
        type(self)._bnd003_skip_reason = None

        def write_batch(suffix, db_or_bucket, src_name, tag, base_ms=None):
            # base_ms lets the caller pin the batch on the window grid; the
            # rows stay `batch_rows` consecutive milliseconds ending at it.
            now_ms = base_ms if base_ms is not None else int(time.time() * 1000)
            batch = [(now_ms - (batch_rows - 1 - k), k + 1, float(k + 1),
                      f"{tag}{k}", 0) for k in range(batch_rows)]
            self._insert_rows(suffix, db_or_bucket, batch, src_name)

        def body(src_name: str):
            mid = _mid_segment(prefix, src_name)
            suffix = src_name[-1]
            db_or_bucket = f"{prefix}_{suffix}db"
            stream = f"s_{src_name}"
            sink = f"sink_{src_name}"
            # _tprev_localtime/_tlocaltime are nanoseconds; InfluxDB stores
            # nanoseconds natively, MySQL/PG timestamps are microsecond-precision.
            div = 1 if suffix == "i" else 1_000
            sink_db = self.NS_DB if suffix == "i" else self.US_DB
            ts_col = _src_ts_col(src_name)

            tdSql.execute(f"USE {self.DB}")
            tdSql.execute(f"DROP STREAM IF EXISTS {stream}")
            tdSql.execute(f"DROP TABLE IF EXISTS {sink_db}.{sink}")
            tdSql.execute(
                f"CREATE STREAM {stream} period(1s) "
                f"FROM {src_name}.{mid}.src_t "
                f"INTO {sink_db}.{sink} AS "
                f"SELECT ts, cnt FROM ("
                f"  SELECT cast(_tprev_localtime/{div} as timestamp) AS ts, "
                f"  COUNT(*) AS cnt, AVG(val) AS avg_val "
                f"  FROM {src_name}.{mid}.src_t "
                f"  WHERE {ts_col} >= cast(_tprev_localtime/{div} as timestamp) "
                f"  AND {ts_col} < cast(_tlocaltime/{div} as timestamp)"
                f") WHERE cnt > 0"
            )
            tdStream.checkStreamStatus(stream)

            # Phase 1 - warm-up.  Ext PERIOD triggers may defer calc dispatch
            # for many seconds on a cold start (especially InfluxDB, and on a
            # loaded CI host where the federated-query environment is still
            # settling), silently skipping windows that closed in the meantime.
            # Keep writing until the first window is aggregated; these rows are
            # not measured.
            warm_timeout_s = 240 if suffix == "i" else 120
            deadline = time.time() + warm_timeout_s
            warm = 0
            while time.time() < deadline:
                write_batch(suffix, db_or_bucket, src_name, f"w{warm}_")
                warm += 1
                time.sleep(1.0)
                if self._bnd003_sink_sum(sink_db, sink)[1] > 0:
                    break
            if self._bnd003_sink_sum(sink_db, sink)[1] <= 0:
                msg = (f"sink {sink} got no window within {warm_timeout_s}s of "
                       f"warm-up ({warm} batch(es) written)")
                if suffix != "i":
                    raise AssertionError(msg)
                # Never treat an empty sink as a pass.  Measure InfluxDB's
                # write-to-visible delay directly first: only a delay beyond
                # the 1s period window explains the empty sink, and that
                # environment limitation is reported as an explicit skip.
                # A fast probe means the reader/bound path is broken.
                delay = self._influx_visible_delay(db_or_bucket)
                tdSql.execute(f"DROP STREAM IF EXISTS {stream}", queryTimes=30)
                tdSql.execute(f"DROP TABLE IF EXISTS {sink_db}.{sink}")
                if delay is None or delay > self._BND003_INFLUX_MAX_VISIBLE_DELAY_S:
                    observed = ("never visible within 10s" if delay is None
                                else f"{delay:.2f}s")
                    reason = (
                        f"{msg}; direct InfluxDB write-to-visible probe: {observed} "
                        f"(WAL flush slower than the 1s period window), so no window "
                        f"can aggregate and the InfluxDB bound scaling is unverified")
                    type(self)._bnd003_skip_reason = reason
                    tdLog.info(f"[{src_name}] {reason}")
                    return
                raise AssertionError(
                    f"{msg}; direct InfluxDB probe became visible in {delay:.2f}s "
                    f"(<= {self._BND003_INFLUX_MAX_VISIBLE_DELAY_S:.1f}s), so the empty "
                    f"sink is not explained by WAL-flush latency")

            # Let every warm-up window close so none of them overlaps the
            # measured rows (empty windows are dropped by cnt > 0).
            base_rows, base_sum = self._bnd003_wait_stable(
                sink_db, sink, stable_s=3.0, timeout_s=30)

            # Phase 2 - measure: a fixed number of batches, ~1s apart.  The
            # last one is pinned to the next whole second so its newest row
            # lands exactly on a period boundary: that row is counted by the
            # window opening at the boundary, never by both sides of it.
            for b in range(n_batches):
                base_ms = None
                if b == n_batches - 1:
                    base_ms = ((int(time.time() * 1000) // 1000) + 1) * 1000
                write_batch(suffix, db_or_bucket, src_name, f"m{b}_", base_ms)
                time.sleep(1.0)
            expected = n_batches * batch_rows

            # Wait for the measured windows to close, then make sure the sum
            # stays put (a duplicated boundary row would show up late).
            rows, total = self._bnd003_wait_stable(
                sink_db, sink, stable_s=3.0, timeout_s=60)
            measured = total - base_sum
            if suffix == "i":
                # WAL-flush latency may hide a batch from its window's calc
                # (see docstring); only double counting is an error here.
                assert 0 <= measured <= expected, \
                    (f"sink {sink} counted {measured} measured row(s), expected "
                     f"at most {expected} (base SUM(cnt)={base_sum}, now {total})")
                tdLog.info(f"[{src_name}] FS-BND-003 verified: {rows - base_rows} "
                           f"window(s), {measured}/{expected} row(s) counted, "
                           f"none twice")
            else:
                assert measured == expected, \
                    (f"sink {sink} counted {measured} measured row(s), expected "
                     f"{expected} (base SUM(cnt)={base_sum}, now {total})")
                assert rows > base_rows, f"sink {sink} produced no measured window"
                tdLog.info(f"[{src_name}] FS-BND-003 verified: {rows - base_rows} "
                           f"window(s), {measured} row(s) counted exactly once")

            tdSql.execute(f"DROP STREAM IF EXISTS {stream}", queryTimes=30)
            tdSql.execute(f"DROP TABLE IF EXISTS {sink_db}.{sink}")

        # Streams reject Oracle external tables (streamTriggerTask's identity
        # check is fail-closed for types past SQL Server), so the stream
        # matrices never provision an Oracle leg.
        self._with_std_sources(prefix, body, skip_oracle=True)

        # Report the InfluxDB environment limitation explicitly instead of
        # silently passing (the other sources already asserted their bounds).
        skip_reason = type(self)._bnd003_skip_reason
        if skip_reason:
            pytest.skip(skip_reason)
