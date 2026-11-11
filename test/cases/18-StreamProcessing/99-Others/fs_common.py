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

# Shared helpers for stream-federated-query tests (FS spec).
# File location: source/taos-community/test/cases/18-StreamProcessing/99-Others/fs_common.py
"""Shared helpers for stream-federated-query tests (FS spec).

Only free-function helpers and one fixture mixin live here; no test classes.
Each test file imports the helpers it needs.
"""

import datetime
import sys
import time

from new_test_framework.utils import tdSql, tdLog

sys.path.insert(0, "cases/09-DataQuerying/19-FederatedQuery")
from federated_query_common import ExtSrcEnv  # noqa: E402


FS_BASE_MS = 1740960000000  # 2025-03-03 00:00:00.000 UTC


def ms_to_dt(ms: int) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(ms / 1000.0)


def dt_str(ms: int) -> str:
    """Convert epoch ms to 'YYYY-MM-DD HH:MM:SS.mmm' in local time."""
    dt = datetime.datetime.fromtimestamp(ms / 1000.0)
    return dt.strftime('%Y-%m-%d %H:%M:%S.') + f"{ms % 1000:03d}"


def oracle_insert_all_sql(rows: list, render_ts, table: str = "SRC_T") -> str:
    """INSERT ALL statement for (ts, val, score, name, flag) rows.

    Oracle 11g has no multi-row VALUES list (23ai added one, 11g rejects it with
    ORA-00933), and a bare string literal would be parsed through the session's
    NLS_TIMESTAMP_FORMAT, so the format is pinned to the millisecond column.
    Every Oracle writer in the stream suite goes through this one builder: two
    copies of the literal form used to exist and would drift apart silently.
    """
    if not rows:
        # An empty batch would render "INSERT ALL  SELECT 1 FROM DUAL", which
        # Oracle rejects with ORA-00908; fail here so the caller sees its own
        # mistake instead of a parser error from the remote side.
        raise ValueError("oracle_insert_all_sql needs at least one row")
    return (
        "INSERT ALL "
        + " ".join(
            f"INTO {table} (ts, val, score, name, flag) VALUES"
            f"(TO_TIMESTAMP('{render_ts(ts)}', 'YYYY-MM-DD HH24:MI:SS.FF3'),"
            f" {val}, {score}, '{name}', {flag})"
            for ts, val, score, name, flag in rows)
        + " SELECT 1 FROM DUAL")


def verify_sink_rows(db: str, stream: str, sink: str, expected: list, label: str,
                     since_ms: int = None) -> None:
    """Wait for `sink` to close all `expected` windows, then assert the exact
    (ts, cnt, avg_val) rows in order.

    `since_ms` restricts the comparison to the measured windows.  It is used by
    cases that established the reader bootstrap with a canary window placed
    before the measured range: the canary rows are not part of the assertion, but
    the measured rows are still compared exactly.
    """
    where = "" if since_ms is None else f" WHERE ts >= '{dt_str(since_ms)}'"
    wait_stream_window_closed(stream, db, sink,
                              expected_rows=len(expected), timeout=120, where=where)
    res = tdSql.getResult(f"SELECT ts, cnt, avg_val FROM {db}.{sink}{where} ORDER BY ts")
    tdSql.checkEqual(res, expected)
    tdLog.info(f"[{label}] verified {len(expected)} sink rows OK")


def ensure_snode(host_id: int = 1) -> None:
    """Idempotent CREATE SNODE; swallow SNODE_ALREADY_DEPLOYED."""
    try:
        tdSql.execute(f"CREATE SNODE ON DNODE {host_id}")
    except Exception as e:
        msg = str(e).lower()
        if "already" in msg or "exists" in msg or "deployed" in msg or "only one" in msg:
            tdLog.info(f"snode on dnode {host_id} already deployed, reuse")
            return
        raise


class StreamWindowNotClosed(TimeoutError):
    """`wait_stream_window_closed` expired; carries the observed row count.

    The row count is what separates the two ways a window can be missing.  Zero
    rows means the reader may have sampled its history cutoff after the batch was
    written, so the whole batch became history and a fresh reader is the only
    recovery.  One or more rows means the window did close and only the expected
    content is missing: rebuilding the stream would delete that evidence, and a
    late cutoff sample can never drop a single row of a window that already
    emitted.
    """

    def __init__(self, message: str, observed_rows: int):
        super().__init__(message)
        self.observed_rows = observed_rows


def wait_stream_window_closed(stream_name: str,
                              result_db: str,
                              result_tbl: str,
                              expected_rows: int,
                              timeout: float = 30.0,
                              poll: float = 0.5,
                              where: str = "") -> None:
    """Block until result table has at least `expected_rows` rows.

    Tolerates transient 'Table does not exist' errors that may occur when the
    stream engine is still creating the sink table (especially for EXT streams
    whose sink table is built asynchronously via the MND transaction).

    `where` (a ready-to-append ` WHERE ...` clause) counts only the rows the
    caller is about to compare, so rows outside that range - a bootstrap canary
    window, for instance - can neither satisfy the threshold early nor hide a
    missing measured window.

    Raises `StreamWindowNotClosed` (a `TimeoutError`) carrying the row count
    observed when the deadline expired: an empty sink can be the reader's
    history-cutoff race, while a non-empty one is a real partial result.
    """
    deadline = time.time() + timeout
    last = -1
    while time.time() < deadline:
        try:
            tdSql.query(sql=f"SELECT COUNT(*) FROM {result_db}.{result_tbl}{where}", queryTimes=30)
            last = tdSql.getData(0, 0) or 0
            if last >= expected_rows:
                tdLog.info(
                    f"stream {stream_name} window closed: rows={last} "
                    f">= expected={expected_rows}"
                )
                return
        except Exception as e:
            msg = str(e).lower()
            if "not exist" in msg or "table" in msg or "out of range" in msg or "0x0112" in msg:
                # Sink table not yet visible or stream still initializing; retry.
                tdLog.info(
                    f"stream {stream_name}: sink table {result_db}.{result_tbl} "
                    f"transient error, retrying ({msg[:80]})"
                )
            else:
                raise
        time.sleep(poll)
    raise StreamWindowNotClosed(
        f"stream {stream_name} did not close enough windows in {timeout}s: "
        f"got {last}, expected {expected_rows}",
        observed_rows=max(last, 0),
    )



def sink_window_rows(result_db: str, result_tbl: str, ts_ms: int) -> int:
    """Rows the sink holds for the window that starts at `ts_ms`."""
    tdSql.query(
        f"SELECT COUNT(*) FROM {result_db}.{result_tbl} "
        f"WHERE ts = '{dt_str(ts_ms)}'", queryTimes=30)
    return tdSql.getData(0, 0) or 0


def wait_ext_reader_bootstrap(stream_name: str,
                              result_db: str,
                              result_tbl: str,
                              *,
                              insert_canary,
                              delete_canary,
                              before_ms: int,
                              slot_gap_ms: int = 3_600_000,
                              attempts: int = 4,
                              per_attempt: float = 30.0,
                              poll: float = 0.5) -> int:
    """Prove the external reader already sampled its history cutoff.

    `checkStreamStatus()` only proves the START request was queued: the EXT
    trigger reader pulls its initial watermark (`MAX(ts)`) afterwards, and a
    batch written before that pull is consumed as history - the later META pulls
    only ask for `ts > maxTs`, so those rows are lost for good and no sink
    timeout can recover them.

    A canary row written after the pull is fetched normally, so an emitted
    canary window proves the bootstrap finished.  Each attempt writes two rows
    one window apart (`ts` and `ts + 60s`) at a slot well before `before_ms`,
    because an INTERVAL(1m) window only closes once a later row arrives; the
    slot and its closer are both deleted again through `delete_canary` as soon
    as the first window reached the sink, so the measured batch sees exactly the
    source it expects.

    A swallowed attempt became history and can never come back, and the reader's
    watermark only moves forward: once the cutoff sampled a slot, every earlier
    row stays invisible (`ts > maxTs`).  Retries therefore step *towards*
    `before_ms`, so each attempt is later than anything a previous sample could
    have consumed; stepping backwards could only waste the remaining attempts.

    The caller keeps the canary windows out of its assertion: they sit before the
    measured range and are excluded with `since_ms`, so every measured window keeps
    its exact expectation.  A closed window emits a COUNT row even when its calc
    finds no input, so the canary slot must never be compared by content.

    Only window-bounded calc SELECTs qualify.  A calc that re-reads the whole
    table or a fixed literal range would include the canary in every measured
    window, so those cases cannot use this barrier.

    `before_ms` must be aligned to a whole minute, because the canary is placed on
    a window start so the sink row it produces has a predictable timestamp.

    Raises `StreamWindowNotClosed` (observed_rows == 0) when no slot was ever
    emitted: the reader is still stuck before its watermark.
    """
    for attempt in range(attempts):
        slot = before_ms - (attempts - attempt) * slot_gap_ms
        slots = (slot, slot + 60_000)
        inserted = []
        emitted = False
        try:
            for ts in slots:
                insert_canary(ts)
                inserted.append(ts)
            deadline = time.time() + per_attempt
            while time.time() < deadline:
                try:
                    if sink_window_rows(result_db, result_tbl, slot) > 0:
                        emitted = True
                        break
                except Exception as e:
                    msg = str(e).lower()
                    # The sink table is created asynchronously for an EXT stream, so
                    # a probe that races its creation is not a failure.
                    if "not exist" not in msg and "0x0112" not in msg and "out of range" not in msg:
                        raise
                time.sleep(poll)
        finally:
            # Never leave a canary behind: the measured batch has to see exactly
            # the source it expects, even when the probe raised.
            for ts in inserted:
                delete_canary(ts)
        if emitted:
            tdLog.info(
                f"stream {stream_name}: external reader bootstrap confirmed by "
                f"canary window {dt_str(slot)} (attempt {attempt + 1})"
            )
            return slot
        tdLog.info(
            f"stream {stream_name}: canary window {dt_str(slot)} was consumed as "
            f"history (attempt {attempt + 1}/{attempts}), retrying at a later slot"
        )
    raise StreamWindowNotClosed(
        f"stream {stream_name}: no canary window was emitted in {attempts} "
        f"attempt(s), the external reader never sampled its history cutoff",
        observed_rows=0,
    )


def get_ext_last_ts(stream_name: str) -> int:
    """Return current ext_last_ts (ms) for the stream's EXT reader task, 0 if none."""
    tdSql.query(
        "SELECT ext_last_ts FROM information_schema.ins_stream_tasks "
        f"WHERE stream_name='{stream_name}' AND task_type IN "
        "('EXT_TRIG_READER','EXT_CALC_READER') ORDER BY ext_last_ts DESC LIMIT 1"
    )
    rows = tdSql.queryResult
    if not rows or rows[0][0] is None:
        return 0
    val = rows[0][0]
    return int(val.timestamp() * 1000) if hasattr(val, "timestamp") else int(val)


def wait_ext_last_ts_advance(stream_name: str,
                             prev_ts: int,
                             timeout: float = 20.0,
                             poll: float = 0.5) -> int:
    """Block until ext_last_ts strictly advances past prev_ts; return new value."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        cur = get_ext_last_ts(stream_name)
        if cur > prev_ts:
            tdLog.info(
                f"stream {stream_name} ext_last_ts advanced "
                f"{prev_ts} -> {cur}"
            )
            return cur
        time.sleep(poll)
    raise TimeoutError(
        f"stream {stream_name} ext_last_ts did not advance past {prev_ts} "
        f"in {timeout}s (still {get_ext_last_ts(stream_name)})"
    )


def verify_sink_data(stream_name: str,
                     result_db: str,
                     result_tbl: str,
                     expected_total_rows: int,
                     expected_cnt_sum: int = 0) -> None:
    """Verify basic correctness of stream sink data.

    Checks:
      1. Sink table has at least expected_total_rows rows.
      2. Every row has cnt > 0 (each window captured at least one source row).
      3. If expected_cnt_sum > 0: SUM(cnt) == expected_cnt_sum (all source rows
         were captured exactly once across all windows).

    Raises AssertionError with a descriptive message on failure.
    """
    # 1. Row count.
    tdSql.query(f"SELECT COUNT(*) FROM {result_db}.{result_tbl}")
    actual_rows = tdSql.getData(0, 0) or 0
    assert actual_rows >= expected_total_rows, (
        f"stream {stream_name}: sink {result_db}.{result_tbl} has {actual_rows} rows, "
        f"expected >= {expected_total_rows}"
    )

    # 2. No zero-count windows.
    tdSql.query(
        f"SELECT COUNT(*) FROM {result_db}.{result_tbl} WHERE cnt <= 0"
    )
    zero_cnt_rows = tdSql.getData(0, 0) or 0
    assert zero_cnt_rows == 0, (
        f"stream {stream_name}: {zero_cnt_rows} window(s) have cnt <= 0 in "
        f"{result_db}.{result_tbl}"
    )

    # 3. Total row count in source matches sum of window counts.
    if expected_cnt_sum > 0:
        tdSql.query(f"SELECT SUM(cnt) FROM {result_db}.{result_tbl}")
        actual_sum = tdSql.getData(0, 0) or 0
        assert actual_sum == expected_cnt_sum, (
            f"stream {stream_name}: SUM(cnt)={actual_sum} in sink "
            f"{result_db}.{result_tbl}, expected {expected_cnt_sum}"
        )
        tdLog.info(
            f"stream {stream_name} data verified: rows={actual_rows} "
            f"SUM(cnt)={actual_sum}"
        )
    else:
        tdLog.info(
            f"stream {stream_name} data verified: rows={actual_rows} "
            f"all cnt>0"
        )


def get_stream_ext_meta(stream_name: str) -> dict:
    """Snapshot ext-related columns from ins_streams + SHOW STREAMS EXT_SOURCES."""
    out: dict = {}
    try:
        tdSql.query(
            f"SELECT message FROM information_schema.ins_streams WHERE stream_name='{stream_name}'"
        )
        if tdSql.queryResult:
            r = tdSql.queryResult[0]
            out["message"] = r[0]
    except Exception as e:
        # Columns may not be present in this build; treat as no errors.
        out["message"] = str(e)
    return out

class FsSharedFixtureMixin:
    """Fixture helpers shared by FS test classes, on top of
    `FederatedQueryTestMixin` (self.DB / self._mysql_cfg() / self._pg_cfg() /
    self._influx_cfg() / self._cleanup_src() / self._mk_influx_real()).

    Mix in alongside FederatedQueryTestMixin:
        class TestFoo(FsSharedFixtureMixin, FederatedQueryTestMixin): ...
    """

    def _delete_rows(self, suffix: str, db_or_bucket: str, ts: int, label: str):
        """Delete an external-source row matching the millisecond timestamp."""
        if suffix == 'm':
            timestamp = dt_str(ts)
            ExtSrcEnv.mysql_exec_cfg(
                self._mysql_cfg(), db_or_bucket,
                [f"DELETE FROM `src_t` WHERE ts = '{timestamp}'"]
            )
            tdLog.info(
                f"[{label}] deleted MySQL row: ts_ms={ts}, ts={timestamp}"
            )
        elif suffix == 'p':
            timestamp = dt_str(ts)
            ExtSrcEnv.pg_exec_cfg(
                self._pg_cfg(), db_or_bucket,
                [f"DELETE FROM public.src_t WHERE ts = '{timestamp}'"]
            )
            tdLog.info(
                f"[{label}] deleted PG row: ts_ms={ts}, ts={timestamp}"
            )
        elif suffix == 's':
            timestamp = dt_str(ts)
            ExtSrcEnv.sqlserver_exec(
                db_or_bucket,
                [f"DELETE FROM dbo.src_t WHERE ts = '{timestamp}'"]
            )
            tdLog.info(
                f"[{label}] deleted SQL Server row: ts_ms={ts}, ts={timestamp}"
            )
        elif suffix == 'i':
            # lines = [
            #     f'src_t val={val}i,score={score},name="{name}",flag={flag}i '
            #     f'{ts * 1_000_000}'
            #     for ts, val, score, name, flag in rows
            # ]
            # ExtSrcEnv.influx_write_cfg(self._influx_cfg(), db_or_bucket, lines)
            tdLog.info(f"[{label}] delete {ts} InfluxDB points")
        elif suffix == 'o':
            # Oracle has one namespace level, so there is no database to name;
            # the table lives in the login schema.
            timestamp = dt_str(ts)
            ExtSrcEnv.oracle_exec_cfg(
                self._oracle_cfg(),
                [f"DELETE FROM SRC_T WHERE ts = "
                 f"TO_TIMESTAMP('{timestamp}', 'YYYY-MM-DD HH24:MI:SS.FF3')",
                 "COMMIT"]
            )
            tdLog.info(
                f"[{label}] deleted Oracle row: ts_ms={ts}, ts={timestamp}")
        else:
            raise RuntimeError(
                f"_delete_rows: unsupported source suffix '{suffix}' ({label})")

    def _can_delete_rows(self, suffix: str) -> bool:
        """Whether `_delete_rows` can undo a write to this source.

        The bootstrap barrier has to remove its canary again before the measured
        batch is written, so a source with no delete path in this harness cannot
        use it.  InfluxDB is the one such source.
        """
        return suffix in ('m', 'p', 's', 'o')

    def _wait_reader_bootstrap(self, *, src_name: str, db_or_bucket: str,
                               stream: str, result_db: str, result_tbl: str,
                               before_ms: int):
        """Prove the EXT reader sampled its history cutoff before any row of the
        measured batch is written (see `wait_ext_reader_bootstrap`).

        Returns the confirmed canary slot (ms), whose sink row is excluded from the
        caller's comparison with `since_ms`, or None when this source has no delete
        path and the barrier therefore cannot undo its canary (InfluxDB): that leg
        keeps the timing it had before Oracle was enabled and must not expect a
        canary window.
        """
        suffix = src_name[-1]
        if not self._can_delete_rows(suffix):
            tdLog.info(
                f"[{src_name}] no delete path for this source: the measured batch "
                f"is written without a bootstrap barrier")
            return None

        def insert_canary(ts_ms: int):
            self._insert_rows(suffix, db_or_bucket,
                              [(ts_ms, 1, 1.0, 'can', 0)], src_name)

        def delete_canary(ts_ms: int):
            self._delete_rows(suffix, db_or_bucket, ts_ms, src_name)

        return wait_ext_reader_bootstrap(
            stream, result_db, result_tbl,
            insert_canary=insert_canary,
            delete_canary=delete_canary,
            before_ms=before_ms,
        )

    def _drop_table(self, suffix: str, db_or_bucket: str, label: str):
        """Drop the trigger table in the external source for the given suffix
        type ('m' MySQL, 'p' PostgreSQL, 's' SQL Server, 'i' InfluxDB, 'o'
        Oracle)."""
        if suffix == 'm':
            ExtSrcEnv.mysql_exec_cfg(
                self._mysql_cfg(), db_or_bucket,
                [f"DROP TABLE `src_t`"]
            )
            tdLog.info(f"[{label}] drop table MySQL")
        elif suffix == 'p':
            # rows_sql = ", ".join(
            #     f"('{dt_str(ts)}', {val}, {score}, '{name}', {flag})"
            #     for ts, val, score, name, flag in rows
            # )
            # ExtSrcEnv.pg_exec_cfg(
            #     self._pg_cfg(), db_or_bucket,
            #     [f"INSERT INTO public.src_t VALUES {rows_sql}"]
            # )
            tdLog.info(f"[{label}] drop table PG")
        elif suffix == 's':
            ExtSrcEnv.sqlserver_exec(db_or_bucket, ["DROP TABLE dbo.src_t"])
            tdLog.info(f"[{label}] drop table SQL Server")
        elif suffix == 'i':
            # lines = [
            #     f'src_t val={val}i,score={score},name="{name}",flag={flag}i '
            #     f'{ts * 1_000_000}'
            #     for ts, val, score, name, flag in rows
            # ]
            # ExtSrcEnv.influx_write_cfg(self._influx_cfg(), db_or_bucket, lines)
            tdLog.info(f"[{label}] drop table InfluxDB")
        elif suffix == 'o':
            # The table may not exist yet (a first run, or a case that failed
            # before its setup): DROP TABLE would then abort the cleanup with
            # ORA-00942, so the drop runs inside a swallowed-exception block.
            ExtSrcEnv.oracle_exec_cfg(
                self._oracle_cfg(),
                ["BEGIN EXECUTE IMMEDIATE 'DROP TABLE SRC_T'; "
                 "EXCEPTION WHEN OTHERS THEN NULL; END;"]
            )
            tdLog.info(f"[{label}] drop table Oracle")
        else:
            raise RuntimeError(
                f"_drop_table: unsupported source suffix '{suffix}' ({label})")

    def _insert_rows(self, suffix: str, db_or_bucket: str, rows: list, label: str):
        """Write rows into the external source for the given suffix type
        ('m' MySQL, 'p' PostgreSQL, 's' SQL Server, 'i' InfluxDB, 'o' Oracle)."""
        if suffix == 'm':
            rows_sql = ", ".join(
                f"('{dt_str(ts)}', {val}, {score}, '{name}', {flag})"
                for ts, val, score, name, flag in rows
            )
            ExtSrcEnv.mysql_exec_cfg(
                self._mysql_cfg(), db_or_bucket,
                [f"INSERT INTO `src_t` VALUES {rows_sql}"]
            )
            tdLog.info(f"[{label}] inserted {len(rows)} MySQL rows")
        elif suffix == 'p':
            rows_sql = ", ".join(
                f"('{dt_str(ts)}', {val}, {score}, '{name}', {flag})"
                for ts, val, score, name, flag in rows
            )
            ExtSrcEnv.pg_exec_cfg(
                self._pg_cfg(), db_or_bucket,
                [f"INSERT INTO public.src_t VALUES {rows_sql}"]
            )
            tdLog.info(f"[{label}] inserted {len(rows)} PG rows")
        elif suffix == 's':
            # db_or_bucket is the remote database; the table lives in dbo.
            rows_sql = ", ".join(
                f"('{dt_str(ts)}', {val}, {score}, '{name}', {flag})"
                for ts, val, score, name, flag in rows
            )
            ExtSrcEnv.sqlserver_exec(
                db_or_bucket,
                [f"INSERT INTO dbo.src_t (ts, val, score, name, flag) "
                 f"VALUES {rows_sql}"]
            )
            tdLog.info(f"[{label}] inserted {len(rows)} SQL Server rows")
        elif suffix == 'i':
            lines = [
                f'src_t val={val}i,score={score},name="{name}",flag={flag}i '
                f'{ts * 1_000_000}'
                for ts, val, score, name, flag in rows
            ]
            ExtSrcEnv.influx_write_cfg(self._influx_cfg(), db_or_bucket, lines)
            tdLog.info(f"[{label}] wrote {len(rows)} InfluxDB points")
        elif suffix == 'o':
            # Oracle has one namespace level (the login schema), so
            # db_or_bucket carries no database name.  11g has no multi-row
            # VALUES list either — the batch goes through INSERT ALL.
            insert_sql = oracle_insert_all_sql(rows, dt_str)
            ExtSrcEnv.oracle_exec_cfg(self._oracle_cfg(), [insert_sql, "COMMIT"])
            tdLog.info(f"[{label}] inserted {len(rows)} Oracle rows")
        else:
            # Never fall through silently: a suffix with no branch used to drop
            # the whole batch, which surfaced much later as a stream that never
            # closed a window instead of as a write error.
            raise RuntimeError(
                f"_insert_rows: unsupported source suffix '{suffix}' ({label})")

    # ------------------------------------------------------------------
    # InfluxDB 4-series (host in {a,b} x region in {x,y}) PARTITION BY
    # fixture family, shared by every test that needs a small multi-uid
    # InfluxDB measurement with a "stale pre-batch, then post-batch" shape
    # (proving the stream does not backfill pre-existing rows).
    # ------------------------------------------------------------------
    _UC_SERIES = (
        ("a", "x", 1), ("a", "y", 3),
        ("b", "x", 2), ("b", "y", 4),
    )
    _UC_PRE_VALUES = {("a", "x"): 10, ("a", "y"): 30, ("b", "x"): 20, ("b", "y"): 40}
    _UC_POST_VALUES = {("a", "x"): 1, ("a", "y"): 3, ("b", "x"): 2, ("b", "y"): 4}

    @classmethod
    def _build_influx_lines(cls, ts_list_ms: list, value_map: dict) -> list:
        """Line-protocol points for `_UC_SERIES` at each ts in `ts_list_ms`,
        with `val` taken from `value_map[(host, region)]`."""
        lines = []
        for ts_ms in ts_list_ms:
            ts_ns = ts_ms * 1_000_000
            for host, region, _ in cls._UC_SERIES:
                lines.append(
                    f'src_t,host={host},region={region},tbname=tname '
                    f'val={value_map[(host, region)]}i,score=1.0,name="x",flag=0i {ts_ns}'
                )
        return lines

    def _prep_partition_influx(self, src: str, i_db: str):
        """Common PARTITION BY-family setup: fresh ext source + Influx DB, then
        prewrite an older "stale" batch before CREATE STREAM. If the stream
        incorrectly backfills historical rows, _UC_PRE_VALUES would leak into
        each test's post-batch assertion and break it."""
        self._cleanup_src(src)
        ExtSrcEnv.influx_create_db_cfg(self._influx_cfg(), i_db)
        pre_lines = self._build_influx_lines(
            [FS_BASE_MS - 180_000, FS_BASE_MS - 120_000, FS_BASE_MS - 60_000],
            self._UC_PRE_VALUES,
        )
        ExtSrcEnv.influx_write_cfg(self._influx_cfg(), i_db, pre_lines)
        self._mk_influx_real(src, database=i_db)

    def _write_partition_post_batch(self, i_db: str):
        """Write the post-CREATE-STREAM verification batch shared by the
        PARTITION BY-family tests: 4 series x 5 timestamps (BASE .. BASE+240s)."""
        post_lines = self._build_influx_lines(
            [FS_BASE_MS + k * 60_000 for k in range(5)], self._UC_POST_VALUES
        )
        ExtSrcEnv.influx_write_cfg(self._influx_cfg(), i_db, post_lines)

    def _teardown_partition_influx(self, src: str, i_db: str):
        self._cleanup_src(src)
        try:
            ExtSrcEnv.influx_drop_db_cfg(self._influx_cfg(), i_db)
        except Exception:
            pass
