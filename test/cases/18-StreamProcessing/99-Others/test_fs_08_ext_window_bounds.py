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
    """For src '{prefix}_{m|p|s|i|o}', return remote db (mysql/influx) or schema (pg/sqlserver/oracle)."""
    suffix = src_name[-1]
    if suffix == "p":
        return "public"
    if suffix == "s":
        # SQL Server 3-segment refs carry the schema in the middle segment;
        # the database comes from the source default (DS §5.3.3).
        return "dbo"
    if suffix == "o":
        # Oracle exposes a single namespace level: the login schema, folded to
        # upper case like every unquoted Oracle identifier.  There is no
        # per-test database - the source's default service is the connection
        # target (DS §5.3.3).
        return ExtSrcEnv.ORACLE_USER.upper()
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

            # The reader samples its history cutoff (MAX(ts)) after the START
            # request is queued, and a row written before that sample becomes
            # history that no later META pull asks for.  Prove the sample already
            # happened with a canary window isolated from the measured ones
            # before the measured rows are written.
            canary_ts = self._wait_reader_bootstrap(
                src_name=src_name, db_or_bucket=db_or_bucket, stream=stream,
                result_db=self.DB, result_tbl=sink, before_ms=self._BASE_MS)

            self._insert_rows(suffix, db_or_bucket, rows, src_name)
            # The canary windows stay out of the comparison: a window that closes
            # without input still emits a COUNT row (cnt=0, avg_val=NULL), so the
            # canary slot and the minutes after it would otherwise be compared too.
            # The measured windows are still compared exactly.
            verify_sink_rows(self.DB, stream, sink, EXPECTED, src_name,
                             since_ms=None if canary_ts is None else self._BASE_MS)

            tdSql.execute(f"DROP STREAM IF EXISTS {stream}", queryTimes=30)
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")

        # Oracle is supported by the ext trigger reader, so every standard
        # source (MySQL/PostgreSQL/InfluxDB/SQL Server/Oracle) is provisioned.
        self._with_std_sources(prefix, body)

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

            canary_ts = self._wait_reader_bootstrap(
                src_name=src_name, db_or_bucket=db_or_bucket, stream=stream,
                result_db=self.DB, result_tbl=sink, before_ms=self._BASE_MS)

            self._insert_rows(suffix, db_or_bucket, rows, src_name)
            # Same as FS-BND-001: the canary windows (including the ones that close
            # without input and still emit a COUNT row) are excluded, the measured
            # windows are compared exactly.
            verify_sink_rows(self.DB, stream, sink, EXPECTED, src_name,
                             since_ms=None if canary_ts is None else self._BASE_MS)

            tdSql.execute(f"DROP STREAM IF EXISTS {stream}", queryTimes=30)
            tdSql.execute(f"DROP TABLE IF EXISTS {self.DB}.{sink}")

        # Oracle is supported by the ext trigger reader, so every standard
        # source (MySQL/PostgreSQL/InfluxDB/SQL Server/Oracle) is provisioned.
        self._with_std_sources(prefix, body)

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

    _BND003_ROUND_TIMEOUT_S = 45

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
    def _bnd003_sink_sum(sink_db: str, sink: str, strict: bool = False):
        """(window rows, SUM(cnt)) of the sink; (0, 0) before it exists.

        ``strict`` re-raises a query failure rather than reporting it as an
        empty sink.  Once warm-up has counted a row the sink is known to
        exist, so a failure past that point means the database or table
        stopped resolving (taosd restarted, or another instance took over the
        port) and must never be read as "no window landed".
        """
        try:
            tdSql.query(f"SELECT COUNT(*), SUM(cnt) FROM {sink_db}.{sink}",
                        queryTimes=1)
        except Exception:
            if strict:
                raise
            return 0, 0
        if not tdSql.queryResult:
            return 0, 0
        return (tdSql.queryResult[0][0] or 0), (tdSql.queryResult[0][1] or 0)

    @staticmethod
    def _bnd003_sink_rows(sink_db: str, sink: str, strict: bool = False):
        """[(window start, cnt)] of the sink; [] before it exists.

        ``measured`` must be a set difference over window labels, never a
        SUM(cnt) delta: a warm-up window that the engine only collects after
        the snapshot would otherwise be charged to the measured phase.

        ``strict`` has the same meaning as in ``_bnd003_sink_sum``: a read
        error is re-raised instead of being indistinguishable from "sink is
        empty", which would otherwise be reported as a period window having
        been skipped.
        """
        try:
            tdSql.query(f"SELECT CAST(ts AS BIGINT), cnt FROM {sink_db}.{sink} ORDER BY ts",
                        queryTimes=1)
        except Exception:
            if strict:
                raise
            return []
        return [(row[0], row[1] or 0) for row in (tdSql.queryResult or [])]

    @staticmethod
    def _bnd003_align_to_tick():
        """Sleep until just after the next whole second.

        Starting a round on a period boundary keeps every measured window
        entirely inside the round, so the window labels seen at the start of
        the round can never be re-labelled by the measurement.
        """
        now = time.time()
        time.sleep(max(0.0, 1.0 - (now - int(now)) + 0.2))

    @classmethod
    def _bnd003_wait_stable(cls, sink_db: str, sink: str, stable_s: float,
                            timeout_s: float):
        """Wait until SUM(cnt) is unchanged for ``stable_s`` seconds.

        Only called once warm-up has counted a row, so the sink must be
        readable; a query failure here aborts instead of settling on (0, 0).
        """
        deadline = time.time() + timeout_s
        last = cls._bnd003_sink_sum(sink_db, sink, strict=True)
        since = time.time()
        while time.time() < deadline:
            time.sleep(0.5)
            cur = cls._bnd003_sink_sum(sink_db, sink, strict=True)
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
        convention as FS-BND-001.  Every measured batch is pinned so its
        newest row sits exactly on a whole second: that row must still be
        counted once, which a closed upper bound would violate. Exact-count
        sources are written ahead of time and independently checked for
        visibility before the first measured window opens, so client startup
        and commit latency cannot masquerade as a bounds failure.

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
            # The base actually used is returned so a failure can report it.
            now_ms = base_ms if base_ms is not None else int(time.time() * 1000)
            batch = [(now_ms - (batch_rows - 1 - k), k + 1, float(k + 1),
                      f"{tag}{k}", 0) for k in range(batch_rows)]
            self._insert_rows(suffix, db_or_bucket, batch, src_name)
            return now_ms

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

            def recreate_and_warmup():
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
                        return False
                    raise AssertionError(
                        f"{msg}; direct InfluxDB probe became visible in {delay:.2f}s "
                        f"(<= {self._BND003_INFLUX_MAX_VISIBLE_DELAY_S:.1f}s), so the empty "
                        f"sink is not explained by WAL-flush latency")

                # Let every warm-up window close so none of them overlaps the
                # measured rows (empty windows are dropped by cnt > 0).
                self._bnd003_wait_stable(sink_db, sink, stable_s=3.0, timeout_s=30)
                return True

            if recreate_and_warmup() is False:
                return

            expected = n_batches * batch_rows
            # Re-baseline on a fresh period boundary.  The round's total is
            # a set difference over window labels, so a warm-up window that
            # only lands now is invisible to it instead of shifting
            # `measured` by a whole batch (the old SUM(cnt) delta did).
            self._bnd003_align_to_tick()
            # Warm-up already counted a window in this table, so any read
            # failure from here on is infrastructure, not a bounds defect.
            try:
                self._bnd003_wait_stable(sink_db, sink, stable_s=2.0,
                                         timeout_s=15)
                # Windows the sink already holds belong to the warm-up: they
                # are excluded by label identity, which works in every unit.
                seen = {ts for ts, _ in self._bnd003_sink_rows(
                    sink_db, sink, strict=True)}
            except Exception as e:
                raise AssertionError(
                    f"sink {sink_db}.{sink} stopped resolving before round "
                    f"measurement ({e}), although warm-up counted "
                    f"its rows seconds earlier. This is not a window-bounds "
                    f"failure: the database or table became unreadable, e.g. "
                    f"taosd was killed and another instance answered on the "
                    f"same port (see the run's sim/dnode1/log/taosdlog.0).") from e

            # Phase 2 - measure.  Exact-count sources must be visible before
            # their first window opens.  Starting a fresh sqlcmd/sqlplus for
            # each batch can take longer than the sub-second lead provided by
            # "next whole second", so that scheme races the PERIOD reader.
            written = []
            if suffix == "i":
                # Preserve the existing InfluxDB WAL-latency coverage.
                for b in range(n_batches):
                    base_ms = ((int(time.time() * 1000) // 1000) + 1) * 1000
                    written.append(write_batch(suffix, db_or_bucket, src_name,
                                               f"m{b}_", base_ms))
                    time.sleep(1.0)
            else:
                # One remote write avoids five client startups.  The rows
                # still straddle adjacent one-second boundaries, including
                # a row exactly on each boundary, with the same 15-row oracle.
                first_base_ms = (int(time.time()) + 10) * 1000
                written = [first_base_ms + b * 1000 for b in range(n_batches)]
                rows = [(base - (batch_rows - 1 - k), k + 1, float(k + 1),
                         f"m{b}_{k}", 0)
                        for b, base in enumerate(written)
                        for k in range(batch_rows)]
                started_ms = int(time.time() * 1000)
                self._insert_rows(suffix, db_or_bucket, rows, src_name)
                write_done_ms = int(time.time() * 1000)
                # This query uses the independent FQ reader connection, not
                # the writer session.  Do not assume a helper's return proves
                # that a window query could already see the committed rows.
                first_window_ms = first_base_ms - 1000
                visible = 0
                while time.time() * 1000 < first_window_ms:
                    tdSql.query(
                        f"SELECT COUNT(*) FROM {src_name}.{mid}.src_t "
                        f"WHERE {ts_col} >= '{dt_str(rows[0][0])}' "
                        f"AND {ts_col} <= '{dt_str(rows[-1][0])}'",
                        queryTimes=1)
                    visible = tdSql.getData(0, 0) or 0
                    if visible == expected:
                        break
                    time.sleep(0.1)
                visible_ms = int(time.time() * 1000)
                tdLog.info(
                    f"[{src_name}] FS-BND-003 preparation: start_ms={started_ms}, "
                    f"write_done_ms={write_done_ms}, checked_ms={visible_ms}, "
                    f"first_window_ms={first_window_ms}, visible={visible}/{expected}")
                assert visible == expected and visible_ms < first_window_ms, (
                    f"FS-BND-003 preparation missed its visibility deadline: "
                    f"source={src_name}, visible={visible}/{expected}, "
                    f"start_ms={started_ms}, write_done_ms={write_done_ms}, "
                    f"checked_ms={visible_ms}, first_window_ms={first_window_ms}; "
                    f"window-bound correctness cannot be evaluated")
                # Wait through the last measured window before inspecting the
                # sink; the existing bounded result polling starts afterwards.
                time.sleep(max(0.0, (written[-1] + 1000) / 1000 - time.time()))

            deadline = time.time() + self._BND003_ROUND_TIMEOUT_S
            last_val, last_change = None, time.time()

            def expected_window_counts():
                """Window labels and counts of the written rows, in time order.

                Rows stay ``batch_rows`` consecutive milliseconds ending on
                the pin, so each row's window is ``floor(t/1000)``; grouping
                in time order is what the sink reports. Labels use the sink
                database's timestamp precision.
                """
                counts = {}
                for base in written:
                    for offset in range(batch_rows - 1, -1, -1):
                        window = (base - offset) // 1000 * 1000
                        counts[window] = counts.get(window, 0) + 1
                scale = 1_000_000 if suffix == "i" else 1_000
                return [(window * scale, counts[window]) for window in sorted(counts)]

            def observed_window_counts():
                """Window labels and counts of this round, in label order."""
                fresh = [(label, cnt) for label, cnt in
                         self._bnd003_sink_rows(sink_db, sink, strict=True)
                         if label not in seen]
                fresh.sort(key=lambda item: item[0])
                return fresh

            while time.time() < deadline:
                measured = sum(cnt for _, cnt in observed_window_counts())
                if measured >= expected:
                    # A duplicated boundary row would land a few seconds
                    # behind, so re-read once before believing the total.
                    time.sleep(3.0)
                    break
                if suffix == "i":
                    # Only "counted at most once" is asserted for InfluxDB,
                    # so stop waiting as soon as the count has stopped
                    # moving instead of burning the whole round timeout.
                    if measured != last_val:
                        last_val, last_change = measured, time.time()
                    elif time.time() - last_change >= 3.0:
                        break
                time.sleep(0.5)

            observed = observed_window_counts()
            measured = sum(cnt for _, cnt in observed)
            windows = len(observed)
            expected_counts = expected_window_counts()
            tdLog.debug(f"[{src_name}] FS-BND-003 windows: observed={observed}, "
                        f"expected={expected_counts}")
            if observed != expected_counts and measured > expected:
                raise AssertionError(
                    f"sink {sink} counted {measured} of {expected} row(s) in window(s) "
                    f"{observed} (expected {expected_counts}): a row on a period "
                    f"boundary was counted twice (batches written at {written})")
            if observed != expected_counts and suffix != "i":
                raise AssertionError(
                    f"sink {sink} window counts {observed} do not match {expected_counts}: "
                    f"rows or entire windows were lost or misplaced "
                    f"(batches written at {written}, "
                    f"sink={self._bnd003_sink_rows(sink_db, sink, strict=True)})")
            if suffix == "i":
                expected_by_window = dict(expected_counts)
                unexpected = [(label, count) for label, count in observed
                              if count > expected_by_window.get(label, 0)]
                if unexpected:
                    raise AssertionError(
                        f"sink {sink} has unexpected window counts {unexpected}; "
                        f"expected at most {expected_counts} (batches written at {written})")
            sink_rows = self._bnd003_sink_rows(sink_db, sink)
            if suffix == "i":
                # WAL-flush latency may hide a batch from its window's calc
                # (see docstring); only double counting is an error here.
                assert 0 <= measured <= expected, \
                    (f"sink {sink} counted {measured} measured row(s), expected "
                     f"at most {expected} (batches written at {written}, "
                     f"sink={sink_rows})")
                tdLog.info(f"[{src_name}] FS-BND-003 verified: {windows} "
                           f"window(s), {measured}/{expected} row(s) counted, "
                           f"none twice")
            else:
                assert measured == expected, \
                    (f"sink {sink} counted {measured} measured row(s), expected "
                     f"{expected}; per-window counts "
                     f"{observed} vs {expected_counts} (batches written at {written}, "
                     f"sink={sink_rows})")
                assert windows > 0, f"sink {sink} produced no measured window"
                tdLog.info(f"[{src_name}] FS-BND-003 verified: {windows} "
                           f"window(s), {measured} row(s) counted exactly once")

            tdSql.execute(f"DROP STREAM IF EXISTS {stream}", queryTimes=30)
            tdSql.execute(f"DROP TABLE IF EXISTS {sink_db}.{sink}")

        # Oracle is supported by the ext trigger reader, so every standard
        # source (MySQL/PostgreSQL/InfluxDB/SQL Server/Oracle) is provisioned.
        self._with_std_sources(prefix, body)

        # Report the InfluxDB environment limitation explicitly instead of
        # silently passing (the other sources already asserted their bounds).
        skip_reason = type(self)._bnd003_skip_reason
        if skip_reason:
            pytest.skip(skip_reason)
