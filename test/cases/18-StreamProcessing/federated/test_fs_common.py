# source/taos-community/test/cases/18-StreamProcessing/federated/test_fs_common.py
"""Shared helpers for stream-federated-query tests (FS spec).

Only helper code lives here; no test classes. Each test file imports the
helpers it needs.
"""

import time
from new_test_framework.utils import tdSql, tdLog


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
            tdSql.query(f"SELECT COUNT(*) FROM {result_db}.{result_tbl}")
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
            "SELECT ext_source_refs, ext_last_ts, ext_error_count, ext_last_error "
            f"FROM information_schema.ins_streams WHERE stream_name='{stream_name}'"
        )
        if tdSql.queryResult:
            r = tdSql.queryResult[0]
            out["ext_source_refs"] = r[0]
            out["ext_last_ts"] = r[1]
            out["ext_error_count"] = r[2]
            out["ext_last_error"] = r[3]
    except Exception as e:
        # Columns may not be present in this build; treat as no errors.
        out["_ext_meta_error"] = str(e)
    try:
        tdSql.query(f"SHOW STREAMS {stream_name} EXT_SOURCES")
        out["ext_sources"] = list(tdSql.queryResult or [])
    except Exception as e:
        out["ext_sources_error"] = str(e)
    return out
