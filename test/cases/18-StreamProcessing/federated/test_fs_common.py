# source/taos-community/test/cases/18-StreamProcessing/federated/test_fs_common.py
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


def verify_sink_rows(db: str, stream: str, sink: str, expected: list, label: str) -> None:
    """Wait for `sink` to close all `expected` windows, then assert the exact
    (ts, cnt, avg_val) rows in order."""
    wait_stream_window_closed(stream, db, sink,
                              expected_rows=len(expected), timeout=120)
    res = tdSql.getResult(f"SELECT ts, cnt, avg_val FROM {db}.{sink} ORDER BY ts")
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


def wait_stream_window_closed(stream_name: str,
                              result_db: str,
                              result_tbl: str,
                              expected_rows: int,
                              timeout: float = 30.0,
                              poll: float = 0.5) -> None:
    """Block until result table has at least `expected_rows` rows.

    Tolerates transient 'Table does not exist' errors that may occur when the
    stream engine is still creating the sink table (especially for EXT streams
    whose sink table is built asynchronously via the MND transaction).
    """
    deadline = time.time() + timeout
    last = -1
    while time.time() < deadline:
        try:
            tdSql.query(sql=f"SELECT COUNT(*) FROM {result_db}.{result_tbl}", queryTimes=30)
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
    raise TimeoutError(
        f"stream {stream_name} did not close enough windows in {timeout}s: "
        f"got {last}, expected {expected_rows}"
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
            # rows_sql = ", ".join(
            #     f"('{dt_str(ts)}', {val}, {score}, '{name}', {flag})"
            #     for ts, val, score, name, flag in rows
            # )
            # ExtSrcEnv.pg_exec_cfg(
            #     self._pg_cfg(), db_or_bucket,
            #     [f"INSERT INTO public.src_t VALUES {rows_sql}"]
            # )
            tdLog.info(f"[{label}] delete {ts} PG rows")
        elif suffix == 'i':
            # lines = [
            #     f'src_t val={val}i,score={score},name="{name}",flag={flag}i '
            #     f'{ts * 1_000_000}'
            #     for ts, val, score, name, flag in rows
            # ]
            # ExtSrcEnv.influx_write_cfg(self._influx_cfg(), db_or_bucket, lines)
            tdLog.info(f"[{label}] delete {ts} InfluxDB points")
    
    def _drop_table(self, suffix: str, db_or_bucket: str, label: str):
        """Write rows into the external source for the given suffix type
        ('m' MySQL, 'p' PostgreSQL, 'i' InfluxDB)."""
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
        elif suffix == 'i':
            # lines = [
            #     f'src_t val={val}i,score={score},name="{name}",flag={flag}i '
            #     f'{ts * 1_000_000}'
            #     for ts, val, score, name, flag in rows
            # ]
            # ExtSrcEnv.influx_write_cfg(self._influx_cfg(), db_or_bucket, lines)
            tdLog.info(f"[{label}] drop table InfluxDB")

    def _insert_rows(self, suffix: str, db_or_bucket: str, rows: list, label: str):
        """Write rows into the external source for the given suffix type
        ('m' MySQL, 'p' PostgreSQL, 'i' InfluxDB)."""
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
        elif suffix == 'i':
            lines = [
                f'src_t val={val}i,score={score},name="{name}",flag={flag}i '
                f'{ts * 1_000_000}'
                for ts, val, score, name, flag in rows
            ]
            ExtSrcEnv.influx_write_cfg(self._influx_cfg(), db_or_bucket, lines)
            tdLog.info(f"[{label}] wrote {len(rows)} InfluxDB points")

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
