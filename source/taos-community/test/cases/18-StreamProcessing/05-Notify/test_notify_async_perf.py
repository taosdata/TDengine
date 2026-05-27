"""
Performance test: async notification must not block stream processing.

Background:
  When on_failure_pause is NOT set (the only currently-supported SQL mode),
  streamDoNotification() takes the async path: JSON is built in the runner
  thread and enqueued to a background worker — network I/O never touches
  the runner hot-path.

Test design:
  Compare two identical event-window streams:
    stream_notify  — sends notifications to a SLOW server (200 ms per msg)
    stream_silent  — no notifications

  Both streams process the same number of windows.
  stream_notify should finish within MAX_OVERHEAD_S of stream_silent.
  Without async, stream_notify would be ~NUM_WINDOWS × DELAY_MS slower.

Since: v3.4.2
Labels: perf,notify
"""

import asyncio
import time
import threading

import websockets

from new_test_framework.utils import tdLog, tdSql, tdStream

# ── tunables ──────────────────────────────────────────────────────────────────
SLOW_PORT       = 19876
NOTIFY_DELAY_MS = 200       # simulated slow server latency
NUM_WINDOWS     = 15        # event windows to trigger per stream
WAIT_TIMEOUT_S  = 90        # max seconds to wait for results
# Async overhead budget: async stream may be at most this much slower
# than silent stream.  Without async the delta would be ~NUM_WINDOWS*0.2s.
MAX_OVERHEAD_S  = NUM_WINDOWS * NOTIFY_DELAY_MS / 1000 * 0.4   # 40% budget

# ── slow mock WebSocket server ────────────────────────────────────────────────
_slow_loop   = None
_slow_server = None
_slow_thread = None
_recv_count  = 0
_recv_lock   = threading.Lock()


async def _slow_handler(ws):
    global _recv_count
    try:
        async for _ in ws:
            await asyncio.sleep(NOTIFY_DELAY_MS / 1000.0)
            with _recv_lock:
                _recv_count += 1
    except Exception:
        pass


async def _run():
    global _slow_server
    _slow_server = await websockets.serve(
        _slow_handler, "0.0.0.0", SLOW_PORT,
        ping_timeout=None, max_size=10 * 1024 * 1024,
    )
    tdLog.info(f"[perf] slow server on port {SLOW_PORT} (delay={NOTIFY_DELAY_MS}ms)")


def _start_slow_server():
    global _slow_loop, _slow_thread
    _slow_loop = asyncio.new_event_loop()

    def _run_loop():
        asyncio.set_event_loop(_slow_loop)
        _slow_loop.run_until_complete(_run())
        _slow_loop.run_forever()

    _slow_thread = threading.Thread(target=_run_loop, name="slow-ws", daemon=True)
    _slow_thread.start()
    time.sleep(0.4)


def _stop_slow_server():
    global _slow_loop, _slow_server, _slow_thread
    if _slow_server:
        _slow_server.close()
    if _slow_loop:
        _slow_loop.call_soon_threadsafe(_slow_loop.stop)
    if _slow_thread:
        _slow_thread.join(timeout=5)


# ── helpers ───────────────────────────────────────────────────────────────────

def _insert_windows(db, tbl, n, start_ts_ms):
    """Insert n event windows (open=f1==1, close=f1==0), 1 s apart."""
    ts = start_ts_ms
    for _ in range(n):
        tdSql.execute(f"insert into {db}.{tbl} values ({ts}, 1, 'o');")
        ts += 1000
        tdSql.execute(f"insert into {db}.{tbl} values ({ts}, 0, 'c');")
        ts += 1000
    return ts


def _wait_rows(db, tbl, expected, timeout=WAIT_TIMEOUT_S):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            tdSql.query(f"select count(*) from {db}.{tbl};")
            cnt = tdSql.getData(0, 0) or 0
            if cnt >= expected:
                return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


# ── test class ────────────────────────────────────────────────────────────────

class TestNotifyAsyncPerf:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_notify_does_not_block_stream(self):
        """Async notification must not block stream runner

        A stream with a SLOW notify URL (200 ms per message) should finish
        processing NUM_WINDOWS windows almost as fast as an identical stream
        with no notification, because the notification is sent asynchronously
        by a background worker thread.

        Without the async worker the stream runner would block on each
        WebSocket send, making it ~NUM_WINDOWS × 200 ms slower.

        Catalog:
            - StreamProcessing:Notify:Performance

        Since: v3.4.2

        Labels: perf,notify

        History:
            - 2026-05-27 Copilot Created
        """
        _start_slow_server()

        # ── setup ──────────────────────────────────────────────────────────────
        tdStream.dropAllStreamsAndDbs()
        tdStream.createSnode()

        for db in ("db_notify", "db_silent"):
            tdSql.execute(f"drop database if exists {db};")
            tdSql.execute(f"create database {db} vgroups 1;")
            tdSql.execute(f"use {db};")
            tdSql.execute(
                "create stable sta (ts timestamp, f1 int, f2 binary(4)) "
                "tags(t1 int);"
            )
            tdSql.execute("create table t1 using sta tags(1);")

        notify_url = f"ws://localhost:{SLOW_PORT}/perf"

        # stream WITH async notification (slow server)
        tdSql.execute("use db_notify;")
        tdSql.execute(
            "create stream stream_notify "
            "event_window(start with f1 = 1 end with f1 = 0) "
            "from t1 "
            f"notify('{notify_url}') on(window_open|window_close) "
            "into res_notify (wstart, cnt) "
            "as select _twstart, count(*) from %%trows;"
        )

        # stream WITHOUT notification (baseline)
        tdSql.execute("use db_silent;")
        tdSql.execute(
            "create stream stream_silent "
            "event_window(start with f1 = 1 end with f1 = 0) "
            "from t1 "
            "into res_silent (wstart, cnt) "
            "as select _twstart, count(*) from %%trows;"
        )

        # wait for both streams to reach Running state on snode
        tdStream.checkStreamStatus("stream_notify")
        tdStream.checkStreamStatus("stream_silent")

        base_ts = int(time.time() * 1000)

        # ── measure: stream WITH notify ────────────────────────────────────────
        tdLog.info(f"[perf] inserting {NUM_WINDOWS} windows → stream_notify ...")
        t0 = time.time()
        _insert_windows("db_notify", "t1", NUM_WINDOWS, base_ts)
        ok_notify = _wait_rows("db_notify", "res_notify", NUM_WINDOWS)
        elapsed_notify = time.time() - t0
        tdLog.info(f"[perf] stream_notify  finished in {elapsed_notify:.2f}s (ok={ok_notify})")

        # ── measure: stream WITHOUT notify ─────────────────────────────────────
        tdLog.info(f"[perf] inserting {NUM_WINDOWS} windows → stream_silent ...")
        t0 = time.time()
        _insert_windows("db_silent", "t1", NUM_WINDOWS, base_ts)
        ok_silent = _wait_rows("db_silent", "res_silent", NUM_WINDOWS)
        elapsed_silent = time.time() - t0
        tdLog.info(f"[perf] stream_silent  finished in {elapsed_silent:.2f}s (ok={ok_silent})")

        # ── correctness check ──────────────────────────────────────────────────
        assert ok_notify, f"stream_notify timed out waiting for {NUM_WINDOWS} rows"
        assert ok_silent, f"stream_silent timed out waiting for {NUM_WINDOWS} rows"

        tdSql.query("select count(*) from db_notify.res_notify;")
        assert (tdSql.getData(0, 0) or 0) >= NUM_WINDOWS, "stream_notify row count mismatch"

        tdSql.query("select count(*) from db_silent.res_silent;")
        assert (tdSql.getData(0, 0) or 0) >= NUM_WINDOWS, "stream_silent row count mismatch"

        # ── performance check ──────────────────────────────────────────────────
        overhead = elapsed_notify - elapsed_silent
        blocking_time = NUM_WINDOWS * NOTIFY_DELAY_MS / 1000.0

        tdLog.info(
            f"[perf] overhead={overhead:.2f}s  "
            f"max_allowed={MAX_OVERHEAD_S:.2f}s  "
            f"would_block_sync={blocking_time:.2f}s"
        )

        assert overhead < MAX_OVERHEAD_S, (
            f"stream_notify is too slow: overhead={overhead:.2f}s > "
            f"max_allowed={MAX_OVERHEAD_S:.2f}s. "
            f"Without async, blocking would be ~{blocking_time:.2f}s. "
            f"The async notification path may not be working."
        )

        # ── verify notifications were actually received ────────────────────────
        # wait for background worker to flush remaining items
        time.sleep(NOTIFY_DELAY_MS / 1000.0 * 3 + 1)
        with _recv_lock:
            recv = _recv_count
        tdLog.info(f"[perf] slow server received {recv} messages")
        # window_open + window_close = 2 per window
        assert recv >= NUM_WINDOWS, (
            f"Expected >= {NUM_WINDOWS} notify messages, got {recv}"
        )

        _stop_slow_server()
        tdLog.info("[perf] test_notify_does_not_block_stream PASSED")
