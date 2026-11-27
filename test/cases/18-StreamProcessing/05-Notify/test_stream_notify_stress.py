# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
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

# -*- coding: utf-8 -*-
"""Stream notify stress test: 50 streams × 1,000 rows.

Test topology
─────────────
  * 50 independent source tables (src_stress_0 … src_stress_49)
  * 50 streams with count_window(1), notify on(window_open|window_close)
  * No INTO (calc_notify_only)
  * One WebSocket URL per stream so the server can attribute messages

Insert strategy
───────────────
  Each stream's 10,000 rows are inserted in BATCH_SIZE-row chunks by a
  dedicated thread, so 50 threads run concurrently.

taosd init (``updatecfgDict`` → ``taos.cfg`` before deploy)
───────────────────────────────────────────────────────────
  Class-level ``updatecfgDict`` seeds notify worker params at taosd start:

  * ``streamNotifyFrameSize``       (KB, default 256)
  * ``streamNotifyWorkerNum``       (default 4; auto TRANGE(cores,1,8) on first start)
  * ``streamNotifyBatchIntervalMs`` (ms, default 5)
  * ``streamNotifyParamPoolSize``   (KB, default 10240; ``tsStreamNotifyParamPoolSize``)

  Override without editing code via env vars before pytest:

  * ``TD_STREAM_NOTIFY_FRAME_SIZE_KB``
  * ``TD_STREAM_NOTIFY_WORKER_NUM``
  * ``TD_STREAM_NOTIFY_BATCH_INTERVAL_MS``
  * ``TD_STREAM_NOTIFY_PARAM_POOL_SIZE_KB``

Network delay (optional TCP proxy, not WS handler sleep)
──────────────────────────────────────────────────────
  Set ``TD_NOTIFY_NET_DELAY_MS=1000`` to insert a transparent TCP proxy
  between taosd and the notify WebSocket server (connect / each send chunk /
  close each delayed by 1 s). Override components with
  ``TD_NOTIFY_NET_CONNECT_DELAY_MS``, ``TD_NOTIFY_NET_TRANSFER_DELAY_MS``,
  ``TD_NOTIFY_NET_CLOSE_DELAY_MS``.

Reported metrics
────────────────
  * total_duration_s      first insert → last notify event received
  * msg_recv_span_s       first WebSocket msg → last WebSocket msg
  * expected_events       50 × 10000 × 2 (OPEN + CLOSE)
  * received_events       notify events parsed from WebSocket payloads
  * received_messages     WebSocket frame count
  * loss_rate_%           (expected − received) / expected × 100
  * throughput_msg_per_s  received_messages / total_duration
  * throughput_evt_per_s  received_events / total_duration
  * avg_events_per_msg    received_events / received_messages

Catalog:
    - Stream

Since: v3.4.1.0

Labels: stress,perf,notify,stream

Jira: None

History:
    - 2026-06-17 Created for notify stress / perf baseline
"""

import asyncio
import logging
import os
import socket
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

import taos
import websockets
import websockets.exceptions as _ws_exc

from new_test_framework.utils import tdCom, tdLog, tdSql, tdStream

_NOTIFY_DIR = os.path.dirname(os.path.abspath(__file__))
if _NOTIFY_DIR not in sys.path:
    sys.path.insert(0, _NOTIFY_DIR)
from notify_delay_proxy import start_notify_delay_proxy, stop_notify_delay_proxy


def _suppress_websockets_test_noise():
    """Avoid pytest 'Captured log call' ERROR/INFO spam from bare TCP probes."""
    for name in ("websockets", "websockets.server", "websockets.protocol", "websockets.client"):
        logging.getLogger(name).setLevel(logging.CRITICAL)


_suppress_websockets_test_noise()


def _wait_tcp_listen(host: str, port: int, timeout: float = 10.0):
    """Wait until *port* is listening without opening a WebSocket handshake."""
    deadline = time.time() + timeout
    host = host or "127.0.0.1"
    while time.time() < deadline:
        try:
            out = subprocess.check_output(
                ["lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN"],
                stderr=subprocess.DEVNULL,
                text=True,
            )
            if out.strip():
                return
        except (subprocess.CalledProcessError, FileNotFoundError):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                if s.connect_ex((host, port)) == 0:
                    return
        time.sleep(0.1)
    raise RuntimeError(f"TCP port {host}:{port} not listening within {timeout}s")


def _taosd_log_path() -> str:
    log_dir = os.path.join(tdCom.work_dir, "dnode1", "log")
    for name in ("taosdlog.0", "taosdlog.1"):
        path = os.path.join(log_dir, name)
        if os.path.isfile(path):
            return path
    return os.path.join(log_dir, "taosdlog.0")


def _taosd_log_line_count() -> int:
    path = _taosd_log_path()
    if not os.path.isfile(path):
        return 0
    try:
        out = subprocess.check_output(
            f"wc -l < '{path}'",
            shell=True,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        return int(out or 0)
    except (ValueError, subprocess.SubprocessError):
        return 0


def _grep_taosd_log_since(key: str, since_line: int) -> int:
    """Count ``key`` occurrences appended after ``since_line`` (1-based)."""
    path = _taosd_log_path()
    if not os.path.isfile(path):
        tdLog.info("[stress] taosd log not found: %s", path)
        return 0
    start = max(1, since_line + 1)
    try:
        out = subprocess.check_output(
            f"tail -n +{start} '{path}' | grep -Fc '{key}' || true",
            shell=True,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        return int(out or 0)
    except (ValueError, subprocess.SubprocessError):
        return 0


def _grep_taosd_log_recent(key: str, tail_bytes: int = 4 * 1024 * 1024) -> int:
    """Fallback: grep recent tail of taosdlog.* (for huge logs)."""
    log_dir = os.path.join(tdCom.work_dir, "dnode1", "log")
    total = 0
    for name in ("taosdlog.1", "taosdlog.0"):
        path = os.path.join(log_dir, name)
        if not os.path.isfile(path):
            continue
        try:
            out = subprocess.check_output(
                f"tail -c {tail_bytes} '{path}' | grep -Fc '{key}' || true",
                shell=True,
                text=True,
                stderr=subprocess.DEVNULL,
            ).strip()
            total += int(out or 0)
        except (ValueError, subprocess.SubprocessError):
            pass
    return total


def _count_param_pool_evicts_since(since_line: int) -> Dict[str, int]:
    """Tally param-pool evictions in taosd log (reclaim + replace + drop)."""
    reclaim = _grep_taosd_log_since("param pool full, reclaim node from", since_line)
    replace = _grep_taosd_log_since("replace oldest pending notify", since_line)
    drop = _grep_taosd_log_since("param pool full, drop new notify", since_line)
    # Fallback when since_line offset is unreliable on multi-GB logs.
    if reclaim + replace + drop == 0:
        reclaim = _grep_taosd_log_recent("param pool full, reclaim node from")
        replace = _grep_taosd_log_recent("replace oldest pending notify")
        drop = _grep_taosd_log_recent("param pool full, drop new notify")
    total = reclaim + replace + drop
    return {
        "reclaim": reclaim,
        "replace": replace,
        "drop": drop,
        "pool_total": total,
        "total": total,
    }


def _count_notify_send_failures_since(since_line: int) -> Dict[str, int]:
    """Tally notify worker send-path failures in taosd log."""
    connect_fail = _grep_taosd_log_since(
        "[notify_worker] get connection for", since_line
    )
    connect_fail += _grep_taosd_log_since(
        "[notify_worker] failed to get connection for", since_line
    )
    retry_fail = _grep_taosd_log_since("failed after reconnect retry", since_line)
    merge_drop = _grep_taosd_log_since(
        "[notify_worker] drop notify item after merge failure", since_line
    )
    if connect_fail + retry_fail + merge_drop == 0:
        connect_fail = _grep_taosd_log_recent(
            "[notify_worker] get connection for"
        )
        connect_fail += _grep_taosd_log_recent(
            "[notify_worker] failed to get connection for"
        )
        retry_fail = _grep_taosd_log_recent("failed after reconnect retry")
        merge_drop = _grep_taosd_log_recent(
            "[notify_worker] drop notify item after merge failure"
        )
    send_total = connect_fail + retry_fail + merge_drop
    return {
        "send_connect_fail": connect_fail,
        "send_retry_fail": retry_fail,
        "send_merge_drop": merge_drop,
        "send_total": send_total,
    }


def _zero_notify_failure_stats() -> Dict[str, int]:
    """Empty failure counters (no taosd log scan)."""
    return {
        "reclaim": 0,
        "replace": 0,
        "drop": 0,
        "pool_total": 0,
        "total": 0,
        "send_connect_fail": 0,
        "send_retry_fail": 0,
        "send_merge_drop": 0,
        "send_total": 0,
        "failure_log_total": 0,
    }


def _count_taosd_notify_failures_since(since_line: int) -> Dict[str, int]:
    """Merge param-pool evict + send-path failures; each category labeled separately."""
    pool = _count_param_pool_evicts_since(since_line)
    send = _count_notify_send_failures_since(since_line)
    merged = {**pool, **send}
    merged["failure_log_total"] = merged["pool_total"] + merged["send_total"]
    return merged


def _notify_failure_stats(
    since_line: int, received_events: int, expected_events: int
) -> Dict[str, int]:
    """Skip expensive taosd log grep when the client already received everything."""
    if expected_events > 0 and received_events >= expected_events:
        return _zero_notify_failure_stats()
    return _count_taosd_notify_failures_since(since_line)


# ── configuration ──────────────────────────────────────────────────────────────
NOTIFY_PORT = 19876          # dedicated port so it never collides with other tests
NOTIFY_BACKEND_PORT_OFFSET = 10000  # WS binds here when delay proxy fronts NOTIFY_PORT
N_STREAMS = 50               # number of parallel streams
N_ROWS = 1000              # rows per stream
BATCH_SIZE = 500             # rows per INSERT batch (20 batches × 500 = 10 000)
TS_STEP_MS = 1               # 1 ms between consecutive rows in one stream
TS_GAP_MS = 100000          # timestamp offset between streams (non-overlapping)
BASE_TS_MS = 1_735_660_800_000  # 2025-01-01 08:00:00.000 CST

# count_window(1) on N_ROWS rows:
#   each row → 1 WINDOW_OPEN + 1 WINDOW_CLOSE  (stream closes last window too)
# Completion: all events received, or stable stall **with taosd pool-evict log**, or WAIT_TIMEOUT_S.
EXPECTED_EVENTS_PER_STREAM = N_ROWS * 2
TOTAL_EXPECTED_EVENTS = N_STREAMS * EXPECTED_EVENTS_PER_STREAM

WAIT_TIMEOUT_S = 600         # max wait for notifications to arrive
STABLE_FOR_S = 10.0          # stall window; early exit when taosd failure logs > 0

CALLER_DIR = os.path.dirname(os.path.realpath(__file__))
DB_NAME = "test_sn_stress"

# taosd.cfg defaults for async notify worker (applied via updatecfgDict at deploy)
DEFAULT_NOTIFY_FRAME_SIZE_KB = 512
DEFAULT_NOTIFY_WORKER_NUM = 8
DEFAULT_NOTIFY_BATCH_INTERVAL_MS = 1
DEFAULT_NOTIFY_PARAM_POOL_SIZE_KB = 15 * 1024  # tsStreamNotifyParamPoolSize default (10 MB)
# ───────────────────────────────────────────────────────────────────────────────


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def _build_notify_updatecfg_dict() -> dict:
    """Build taosd init cfg for notify worker params (written to taos.cfg at deploy)."""
    return {
        "streamNotifyFrameSize": str(
            _env_int("TD_STREAM_NOTIFY_FRAME_SIZE_KB", DEFAULT_NOTIFY_FRAME_SIZE_KB)
        ),
        "streamNotifyWorkerNum": str(
            _env_int("TD_STREAM_NOTIFY_WORKER_NUM", DEFAULT_NOTIFY_WORKER_NUM)
        ),
        "streamNotifyBatchIntervalMs": str(
            _env_int(
                "TD_STREAM_NOTIFY_BATCH_INTERVAL_MS",
                DEFAULT_NOTIFY_BATCH_INTERVAL_MS,
            )
        ),
        "streamNotifyParamPoolSize": str(
            _env_int(
                "TD_STREAM_NOTIFY_PARAM_POOL_SIZE_KB",
                DEFAULT_NOTIFY_PARAM_POOL_SIZE_KB,
            )
        ),
    }


def _env_int_optional(name: str) -> Optional[int]:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return None
    return int(raw)


def _parse_net_delay_cfg() -> dict:
    """Return TCP proxy delay config in milliseconds (0 = disabled)."""
    base_ms = _env_int("TD_NOTIFY_NET_DELAY_MS", 0)
    connect_ms = _env_int_optional("TD_NOTIFY_NET_CONNECT_DELAY_MS")
    transfer_ms = _env_int_optional("TD_NOTIFY_NET_TRANSFER_DELAY_MS")
    close_ms = _env_int_optional("TD_NOTIFY_NET_CLOSE_DELAY_MS")
    return {
        "enabled": base_ms > 0 or any(x is not None for x in (connect_ms, transfer_ms, close_ms)),
        "connect_ms": connect_ms if connect_ms is not None else base_ms,
        "transfer_ms": transfer_ms if transfer_ms is not None else base_ms,
        "close_ms": close_ms if close_ms is not None else base_ms,
    }


class _NotifyServerStack:
    """WebSocket perf server, optionally fronted by a TCP delay proxy."""

    def __init__(self, public_port: int):
        self.public_port = public_port
        self._delay_cfg = _parse_net_delay_cfg()
        self._use_proxy = self._delay_cfg["enabled"]
        backend_port = (
            public_port + NOTIFY_BACKEND_PORT_OFFSET if self._use_proxy else public_port
        )
        self._server = _PerfServer(backend_port)
        self._proxy = None

    def start(self):
        self._server.start()
        if self._use_proxy:
            tdLog.info(
                "[stress] TCP delay proxy %d -> %d "
                "(connect=%dms transfer=%dms close=%dms)",
                self.public_port,
                self._server._port,
                self._delay_cfg["connect_ms"],
                self._delay_cfg["transfer_ms"],
                self._delay_cfg["close_ms"],
            )
            self._proxy = start_notify_delay_proxy(
                listen_port=self.public_port,
                backend_port=self._server._port,
                connect_delay_ms=self._delay_cfg["connect_ms"],
                transfer_delay_ms=self._delay_cfg["transfer_ms"],
                close_delay_ms=self._delay_cfg["close_ms"],
            )
        _wait_tcp_listen("127.0.0.1", self.public_port)

    def stop(self):
        stop_notify_delay_proxy()
        self._server.stop()

    def event_count(self) -> int:
        return self._server.event_count()

    def msg_count(self) -> int:
        return self._server.msg_count()

    def get_records(self) -> List[dict]:
        return self._server.get_records()

    def msg_recv_span_s(self) -> Optional[float]:
        return self._server.msg_recv_span_s()

    def wait_until_stable(self, *args, **kwargs) -> int:
        return self._server.wait_until_stable(*args, **kwargs)


# ── in-memory WebSocket server ─────────────────────────────────────────────────

class _PerfServer:
    """Minimal async WebSocket server that records (recv_ms, raw_msg) per path.

    Thread-safe: the asyncio loop runs in a background thread; the test thread
    calls ``wait_until_stable()`` and ``get_events()`` from outside.
    """

    def __init__(self, port: int):
        self._port = port
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._server = None
        self._thread: Optional[threading.Thread] = None
        self._stop_event: Optional[asyncio.Event] = None

        self._lock = threading.Lock()
        # list of {"recv_ms": float, "stream_idx": int, ...}
        self._records: List[dict] = []
        self._msg_count = 0
        self._first_msg_ms: Optional[float] = None
        self._last_msg_ms: Optional[float] = None

    # ------------------------------------------------------------------
    # Async core
    # ------------------------------------------------------------------

    async def _handler(self, ws):
        raw_path = getattr(ws, "path", None)
        if raw_path is None:
            req = getattr(ws, "request", None)
            raw_path = getattr(req, "path", "/") if req else "/"
        path = (raw_path or "/").lstrip("/")  # e.g. "stress_7"
        # Extract stream index from path "stress_<i>"
        try:
            stream_idx = int(path.split("_")[-1])
        except (ValueError, IndexError):
            stream_idx = -1

        try:
            async for message in ws:
                recv_ms = time.time() * 1000.0
                self._ingest(message, stream_idx, recv_ms)
        except (_ws_exc.ConnectionClosedOK,
                _ws_exc.ConnectionClosedError,
                _ws_exc.ConnectionClosed):
            pass
        except Exception:
            pass

    def _ingest(self, message: str, stream_idx: int, recv_ms: float):
        import json
        try:
            data = json.loads(message)
        except Exception:
            return
        msg_ts = data.get("timestamp", 0)
        records = []
        for stream_obj in data.get("streams", []):
            for ev in stream_obj.get("events", []):
                records.append({
                    "recv_ms": recv_ms,
                    "msg_ts": msg_ts,
                    "stream_idx": stream_idx,
                    "event_type": ev.get("eventType", ""),
                    "event_time_ms": ev.get("eventTime", 0),
                    "window_start": ev.get("windowStart", 0),
                    "wstart": ev.get("wstart", 0),
                })
        with self._lock:
            self._records.extend(records)
            self._msg_count += 1
            if self._first_msg_ms is None:
                self._first_msg_ms = recv_ms
            self._last_msg_ms = recv_ms

    async def _run(self):
        self._stop_event = asyncio.Event()
        self._server = await websockets.serve(
            self._handler,
            "0.0.0.0",
            self._port,
            ping_interval=30,
            ping_timeout=None,
            max_size=32 * 1024 * 1024,
            logger=logging.getLogger("websockets.server"),
        )
        await self._stop_event.wait()
        self._server.close()
        await self._server.wait_closed()

    def _thread_main(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._run())
        self._loop.close()

    # ------------------------------------------------------------------
    # Public API (called from test thread)
    # ------------------------------------------------------------------

    def start(self):
        self._thread = threading.Thread(target=self._thread_main, daemon=True)
        self._thread.start()
        _wait_tcp_listen("127.0.0.1", self._port)

    def stop(self):
        if self._loop and self._stop_event:
            self._loop.call_soon_threadsafe(self._stop_event.set)
        if self._thread:
            self._thread.join(timeout=5.0)

    def event_count(self) -> int:
        with self._lock:
            return len(self._records)

    def msg_count(self) -> int:
        with self._lock:
            return self._msg_count

    def get_records(self) -> List[dict]:
        with self._lock:
            return list(self._records)

    def msg_recv_span_s(self) -> Optional[float]:
        """Wall-clock span from first WebSocket frame to last (seconds)."""
        with self._lock:
            if self._first_msg_ms is None or self._last_msg_ms is None:
                return None
            return (self._last_msg_ms - self._first_msg_ms) / 1000.0

    def wait_until_stable(
        self,
        expected_events: int,
        total_timeout_s: float = WAIT_TIMEOUT_S,
        stable_for_s: float = STABLE_FOR_S,
        log_line_before: int = 0,
    ) -> int:
        """Wait until all events arrive, pool-evict stall, or timeout.

        A stable window (no new WS events) ends the wait when taosd logs show
        param-pool eviction or send-path failures. Otherwise the worker may
        still be draining backlog and we keep waiting.
        """
        deadline = time.time() + total_timeout_s
        last_count = -1
        stable_since: Optional[float] = None
        last_log = 0.0
        stall_keep_waiting_logged = False

        while time.time() < deadline:
            cur = self.event_count()
            now = time.time()

            if cur >= expected_events:
                tdLog.info("[stress] received all %d expected events", expected_events)
                return cur

            if cur != last_count:
                last_count = cur
                stable_since = now
                stall_keep_waiting_logged = False
                if now - last_log >= 10.0:
                    tdLog.info(
                        "[stress] progress: %d / %d events (%.1f%%)",
                        cur, expected_events, cur * 100.0 / expected_events,
                    )
                    last_log = now
            elif (
                stable_for_s > 0
                and stable_since is not None
                and (now - stable_since) >= stable_for_s
            ):
                fail = _count_taosd_notify_failures_since(log_line_before)
                ratio = cur * 100.0 / expected_events if expected_events else 0.0
                if fail["pool_total"] > 0 or fail["send_total"] > 0:
                    tdLog.info(
                        "[stress] no new events for %.0fs — taosd failures: "
                        "pool=%d (reclaim=%d replace=%d drop=%d), "
                        "send=%d (connect=%d retry=%d merge_drop=%d), merged=%d — "
                        "received %d / %d (%.1f%%)",
                        stable_for_s,
                        fail["pool_total"],
                        fail["reclaim"],
                        fail["replace"],
                        fail["drop"],
                        fail["send_total"],
                        fail["send_connect_fail"],
                        fail["send_retry_fail"],
                        fail["send_merge_drop"],
                        fail["failure_log_total"],
                        cur,
                        expected_events,
                        ratio,
                    )
                    break
                if not stall_keep_waiting_logged:
                    tdLog.info(
                        "[stress] no new events for %.0fs but taosd failure logs=0 — "
                        "keep waiting for drain (timeout %.0fs)",
                        stable_for_s,
                        total_timeout_s,
                    )
                    stall_keep_waiting_logged = True
                stable_since = now
            time.sleep(0.5)
        else:
            tdLog.info(
                "[stress] wait timeout after %.0fs — received %d / %d events",
                total_timeout_s, last_count, expected_events,
            )
        return last_count


# ── report ─────────────────────────────────────────────────────────────────────

def _print_report(
    n_streams: int,
    n_rows: int,
    expected_events: int,
    received_events: int,
    total_msg: int,
    total_duration_s: float,
    msg_recv_span_s: Optional[float],
    title: str = "STREAM NOTIFY STRESS TEST REPORT",
    notify_cfg: Optional[dict] = None,
    failure_stats: Optional[dict] = None,
):
    sep = "=" * 76
    print(sep, flush=True)
    print(f"  {title}", flush=True)
    print(sep, flush=True)
    if notify_cfg:
        print(
            f"  streamNotifyFrameSize       : {notify_cfg.get('streamNotifyFrameSize', 'N/A')} KB",
            flush=True,
        )
        print(
            f"  streamNotifyWorkerNum       : {notify_cfg.get('streamNotifyWorkerNum', 'N/A')}",
            flush=True,
        )
        print(
            f"  streamNotifyBatchIntervalMs : {notify_cfg.get('streamNotifyBatchIntervalMs', 'N/A')} ms",
            flush=True,
        )
        print(
            f"  streamNotifyParamPoolSize   : {notify_cfg.get('streamNotifyParamPoolSize', 'N/A')} KB",
            flush=True,
        )
        print(sep, flush=True)
    print(f"  Streams              : {n_streams}", flush=True)
    print(f"  Rows / stream        : {n_rows}", flush=True)
    print(f"  Total rows inserted  : {n_streams * n_rows:,}", flush=True)
    print(sep, flush=True)
    loss = max(0, expected_events - received_events)
    loss_rate = loss / expected_events * 100 if expected_events else 0.0
    print(f"  Expected events      : {expected_events:,}", flush=True)
    print(f"  Received events      : {received_events:,}", flush=True)
    print(f"  Received messages    : {total_msg:,}", flush=True)
    print(f"  Unreceived events    : {loss:,}", flush=True)
    print(f"  Unreceived rate      : {loss_rate:.2f}%", flush=True)
    if failure_stats is not None:
        pool_total = failure_stats.get("pool_total", 0)
        send_total = failure_stats.get("send_total", 0)
        failure_log_total = failure_stats.get("failure_log_total", pool_total + send_total)
        unexplained = max(0, loss - pool_total)
        print(sep, flush=True)
        print("  taosd notify failure signals (since insert):", flush=True)
        print(
            f"    [pool] reclaim evict   : {failure_stats.get('reclaim', 0):,}",
            flush=True,
        )
        print(
            f"    [pool] replace evict   : {failure_stats.get('replace', 0):,}",
            flush=True,
        )
        print(
            f"    [pool] drop evict      : {failure_stats.get('drop', 0):,}",
            flush=True,
        )
        print(f"    [pool] subtotal         : {pool_total:,}", flush=True)
        print(
            f"    [send] connect fail     : {failure_stats.get('send_connect_fail', 0):,}",
            flush=True,
        )
        print(
            f"    [send] retry exhausted   : {failure_stats.get('send_retry_fail', 0):,}",
            flush=True,
        )
        print(
            f"    [send] merge drop        : {failure_stats.get('send_merge_drop', 0):,}",
            flush=True,
        )
        print(f"    [send] subtotal          : {send_total:,}", flush=True)
        print(f"    merged log total         : {failure_log_total:,}", flush=True)
        print(
            "    (send counts are log lines; each may pop a merged batch, not 1:1 events)",
            flush=True,
        )
        print(sep, flush=True)
        print(
            f"  unexplained gap (client) : {unexplained:,}"
            f"  (unreceived − pool evict)",
            flush=True,
        )
        if pool_total > 0:
            match = pool_total == loss
            print(
                f"  pool evict == loss       : {'yes' if match else 'NO'}",
                flush=True,
            )
        if send_total > 0 and loss > 0:
            print(
                f"  send failures present    : yes"
                f"  (likely explains gap when pool evict=0)",
                flush=True,
            )
    print(sep, flush=True)
    print(
        f"  Total duration       : {total_duration_s:.3f} s"
        f"  (first insert → last event)",
        flush=True,
    )
    if msg_recv_span_s is not None:
        print(
            f"  Msg recv span        : {msg_recv_span_s:.3f} s"
            f"  (first msg → last msg)",
            flush=True,
        )
        head_gap = total_duration_s - msg_recv_span_s
        print(
            f"  Duration − msg span  : {head_gap:.3f} s"
            f"  (insert→1st msg + last msg→last event)",
            flush=True,
        )
    else:
        print("  Msg recv span        : N/A", flush=True)
    print(sep, flush=True)
    print(f"  Throughput (msgs/s)  : {total_msg / total_duration_s:.1f}", flush=True)
    print(f"  Throughput (evts/s)  : {received_events / total_duration_s:.1f}", flush=True)
    if total_msg > 0:
        print(f"  Avg events / msg     : {received_events / total_msg:.1f}", flush=True)
    if msg_recv_span_s and msg_recv_span_s > 0:
        print(
            f"  Msgs/s (msg span)    : {total_msg / msg_recv_span_s:.1f}",
            flush=True,
        )
        print(
            f"  Evts/s (msg span)    : {received_events / msg_recv_span_s:.1f}",
            flush=True,
        )
    print(sep, flush=True)


# ── test class ─────────────────────────────────────────────────────────────────

class TestStreamNotifyStress:
    """Stress / throughput tests for stream notify (calc_notify_only + async worker).

    Run with::

        pytest test_stream_notify_stress.py -v -s

    Optional taosd init overrides (applied to taos.cfg before deploy)::

        TD_STREAM_NOTIFY_FRAME_SIZE_KB=256 \\
        TD_STREAM_NOTIFY_WORKER_NUM=4 \\
        TD_STREAM_NOTIFY_BATCH_INTERVAL_MS=5 \\
        TD_STREAM_NOTIFY_PARAM_POOL_SIZE_KB=1024 \\
        pytest test_stream_notify_stress.py -v -s

    The ``-s`` flag is needed to see the printed benchmark report.
    """

    # Written into taos.cfg by conftest before taosd deploy/restart.
    updatecfgDict = _build_notify_updatecfg_dict()

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdLog.info("[stress] taosd notify init cfg (updatecfgDict): %s", cls.updatecfgDict)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _log_notify_dnode_vars(expected: dict):
        """Log effective dnode variables (sanity check after taosd init)."""
        for key in (
            "streamNotifyFrameSize",
            "streamNotifyWorkerNum",
            "streamNotifyBatchIntervalMs",
            "streamNotifyParamPoolSize",
        ):
            tdSql.query(f"show dnode 1 variables like '{key}';")
            if tdSql.getRows() <= 0:
                tdLog.info("[stress] dnode variable %s not found", key)
                continue
            actual = tdSql.getData(0, 2)
            tdLog.info("[stress] dnode %s = %s (cfg init %s)", key, actual, expected.get(key))
            if str(actual) != str(expected.get(key)):
                tdLog.info(
                    "[stress] WARNING: %s mismatch: expected %s, got %s",
                    key,
                    expected.get(key),
                    actual,
                )

    @staticmethod
    def _drop_db(db_name: str):
        try:
            tdSql.execute(f"drop database if exists {db_name}")
        except Exception as e:
            if "virtual table stream exists" in str(e).lower():
                tdSql.execute(f"drop database if exists {db_name} force")
            else:
                raise

    @staticmethod
    def _ensure_snode():
        tdSql.query("show snodes")
        if tdSql.getRows() == 0:
            tdStream.createSnode()

    @staticmethod
    def _create_streams(n: int, port: int, db_name: str = DB_NAME):
        """Create n source tables and n count_window(1) sync-notify streams."""
        tdSql.execute(f"use {db_name}")
        for i in range(n):
            src = f"src_stress_{i}"
            stream = f"s_stress_{i}"
            url = f"ws://127.0.0.1:{port}/stress_{i}"
            tdSql.execute(f"create table {src}(ts timestamp, v int)")
            tdSql.execute(
                f"create stream {stream} "
                f"count_window(1) from {src} "
                f"stream_options(calc_notify_only) "
                f"notify('{url}') on(window_open|window_close) "
                f"as select _twstart as wstart, count(*) as cnt from %%trows"
            )
        # Wait for all streams to be ready
        for i in range(n):
            tdStream.checkStreamStatus(f"s_stress_{i}")

    @staticmethod
    def _insert_stream_rows(
        stream_idx: int,
        n_rows: int,
        batch_size: int,
        base_ts_ms: int,
        ts_step_ms: int,
        db_name: str = DB_NAME,
    ):
        """Insert n_rows for one stream using a dedicated taos connection."""
        src = f"src_stress_{stream_idx}"
        conn = taos.connect(config=tdCom.getClientCfgPath(), database=db_name)
        cur = conn.cursor()
        try:
            for batch_start in range(0, n_rows, batch_size):
                batch_end = min(batch_start + batch_size, n_rows)
                vals = ", ".join(
                    f"({base_ts_ms + j * ts_step_ms}, {j})"
                    for j in range(batch_start, batch_end)
                )
                cur.execute(f"insert into {src} values {vals}")
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def _verify_row_counts(n_streams: int, n_rows: int, db_name: str = DB_NAME):
        """Ensure every source table received all rows."""
        tdSql.execute(f"use {db_name}")
        bad = []
        for i in range(n_streams):
            tdSql.query(f"select count(*) from src_stress_{i}")
            cnt = tdSql.getData(0, 0)
            if cnt != n_rows:
                bad.append((i, cnt))
        if bad:
            sample = ", ".join(f"stream_{i}={cnt}" for i, cnt in bad[:5])
            raise AssertionError(
                f"[stress] insert verification failed for {len(bad)} streams "
                f"(expected {n_rows} rows each). sample: {sample}"
            )

    @staticmethod
    def _run_benchmark(
        n_streams: int,
        n_rows: int,
        batch_size: int,
        port: int,
        db_name: str,
        wait_timeout_s: float,
        stable_for_s: float,
        report_title: str,
        done_msg: str,
        notify_cfg: Optional[dict] = None,
    ):
        server = _NotifyServerStack(port)
        t_first_insert_ms: float = 0.0
        t_last_recv_ms: float = 0.0
        expected_events = n_streams * n_rows * 2
        if notify_cfg is None:
            notify_cfg = TestStreamNotifyStress.updatecfgDict

        try:
            tdLog.info("[stress] Starting WebSocket server on port %d", port)
            server.start()

            TestStreamNotifyStress._ensure_snode()
            TestStreamNotifyStress._log_notify_dnode_vars(notify_cfg)
            TestStreamNotifyStress._drop_db(db_name)
            tdSql.execute(f"create database {db_name} vgroups 1")
            tdSql.execute(f"use {db_name}")

            tdLog.info("[stress] Creating %d sync-notify streams…", n_streams)
            TestStreamNotifyStress._create_streams(n_streams, port, db_name)
            tdLog.info("[stress] All streams ready")

            tdLog.info(
                "[stress] Inserting %d streams × %d rows (%d total), batch=%d…",
                n_streams, n_rows, n_streams * n_rows, batch_size,
            )
            t_first_insert_ms = time.time() * 1000.0
            log_line_before = _taosd_log_line_count()

            with ThreadPoolExecutor(max_workers=n_streams) as pool:
                futures = {
                    pool.submit(
                        TestStreamNotifyStress._insert_stream_rows,
                        i,
                        n_rows,
                        batch_size,
                        BASE_TS_MS + i * TS_GAP_MS,
                        TS_STEP_MS,
                        db_name,
                    ): i
                    for i in range(n_streams)
                }
                for fut in as_completed(futures):
                    i = futures[fut]
                    try:
                        fut.result()
                    except Exception as exc:
                        tdLog.error("[stress] Stream %d insert failed: %s", i, exc)

            t_inserts_done_ms = time.time() * 1000.0
            tdLog.info(
                "[stress] All inserts done in %.2f s",
                (t_inserts_done_ms - t_first_insert_ms) / 1000.0,
            )
            TestStreamNotifyStress._verify_row_counts(n_streams, n_rows, db_name)
            tdLog.info("[stress] Row-count verification passed")

            tdLog.info(
                "[stress] Waiting up to %d s for notify drain "
                "(stable %.0fs ends early when taosd pool/send failure logs > 0)…",
                int(wait_timeout_s), stable_for_s,
            )
            received = server.wait_until_stable(
                expected_events, wait_timeout_s, stable_for_s, log_line_before
            )
            records = server.get_records()
            t_last_recv_ms = (
                max(r["recv_ms"] for r in records)
                if records else time.time() * 1000.0
            )
            total_msg = server.msg_count()
            msg_recv_span_s = server.msg_recv_span_s()
            tdLog.info(
                "[stress] Received %d events, %d messages, msg span %.3f s",
                received, total_msg, msg_recv_span_s or 0.0,
            )

            total_duration_s = (t_last_recv_ms - t_first_insert_ms) / 1000.0
            lost_events = max(0, expected_events - received)
            failure_stats = _notify_failure_stats(
                log_line_before, received, expected_events
            )
            if received >= expected_events:
                tdLog.info(
                    "[stress] full receive (%d/%d) — skip taosd log failure scan",
                    received, expected_events,
                )
            tdLog.info(
                "[stress] taosd log %s — failures since insert: "
                "pool(reclaim=%d replace=%d drop=%d total=%d) "
                "send(connect=%d retry=%d merge_drop=%d total=%d) "
                "merged=%d client unreceived=%d",
                _taosd_log_path(),
                failure_stats["reclaim"],
                failure_stats["replace"],
                failure_stats["drop"],
                failure_stats["pool_total"],
                failure_stats["send_connect_fail"],
                failure_stats["send_retry_fail"],
                failure_stats["send_merge_drop"],
                failure_stats["send_total"],
                failure_stats["failure_log_total"],
                lost_events,
            )
            _print_report(
                n_streams=n_streams,
                n_rows=n_rows,
                expected_events=expected_events,
                received_events=received,
                total_msg=total_msg,
                total_duration_s=total_duration_s,
                msg_recv_span_s=msg_recv_span_s,
                title=report_title,
                notify_cfg=notify_cfg,
                failure_stats=failure_stats,
            )

            if failure_stats["pool_total"] > 0:
                if lost_events > 0:
                    tdSql.checkEqual(failure_stats["pool_total"], lost_events)
                else:
                    tdLog.exit(
                        "[stress] taosd reported %d pool evictions but client unreceived 0",
                        failure_stats["pool_total"],
                    )
            elif failure_stats["send_total"] > 0 and lost_events > 0:
                tdLog.info(
                    "[stress] unreceived %d events with send failures "
                    "(connect=%d retry=%d merge_drop=%d log lines; not 1:1 with events)",
                    lost_events,
                    failure_stats["send_connect_fail"],
                    failure_stats["send_retry_fail"],
                    failure_stats["send_merge_drop"],
                )
            elif lost_events > 0:
                tdLog.info(
                    "[stress] unreceived %d events but taosd failure logs=0 — "
                    "slow drain or wait timeout",
                    lost_events,
                )

            loss_rate = lost_events / expected_events if expected_events else 0.0
            if loss_rate > 0.05 and failure_stats["failure_log_total"] == 0:
                tdLog.info(
                    "[stress] WARNING: unreceived %.2f%% without taosd failure logs "
                    "(expected %d, got %d) — increase WAIT_TIMEOUT or check drain",
                    loss_rate * 100, expected_events, received,
                )
            elif loss_rate > 0.05:
                tdLog.info(
                    "[stress] WARNING: unreceived %.2f%% "
                    "(expected %d, got %d, pool=%d send=%d merged=%d)",
                    loss_rate * 100,
                    expected_events,
                    received,
                    failure_stats["pool_total"],
                    failure_stats["send_total"],
                    failure_stats["failure_log_total"],
                )
            tdLog.info(done_msg)

        finally:
            server.stop()
            TestStreamNotifyStress._drop_db(db_name)

    # ------------------------------------------------------------------
    # Test entry point
    # ------------------------------------------------------------------

    def test_stress_50_streams_10k_rows(self):
        """Stress test: 50 streams × 10,000 rows, async notify worker throughput.

        Stress test: 50 streams × 10,000 rows, async notify worker throughput.

        Catalog:
            - Stream

        Since: v3.4.1.0

        Labels: stress,perf,notify,stream

        Jira: None

        History:
            - 2026-06-17 Created

        """
        self._run_benchmark(
            n_streams=N_STREAMS,
            n_rows=N_ROWS,
            batch_size=BATCH_SIZE,
            port=NOTIFY_PORT,
            db_name=DB_NAME,
            wait_timeout_s=WAIT_TIMEOUT_S,
            stable_for_s=STABLE_FOR_S,
            report_title="STREAM NOTIFY STRESS TEST REPORT (async worker)",
            done_msg="test_stress_50_streams_10k_rows ............. [ done ]",
        )

