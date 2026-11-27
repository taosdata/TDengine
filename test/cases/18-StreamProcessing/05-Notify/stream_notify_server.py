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
"""Minimal WebSocket server for stream notify integration tests.

Ping-pong contract (aligned with tcurl.c)
-----------------------------------------
taosd (libcurl CONNECT_ONLY) keeps one persistent WS per notify URL:

  * Client probe — after 30 s idle (TCURL_WS_PING_IDLE_MS) the next send runs
    tcurlEnsureAlive(), which may issue a client PING (payload ``tdnotify``)
    and wait up to 2 s for PONG.
  * Client drain — on every send, tcurlDrainRecv() reads inbound frames and
    auto-replies server PING with PONG.
  * Server probe — this test server may send keepalive PINGs, but must NOT
    close the link when pong is late: taosd only recv-drains on the next
    notify send, not in a background thread.

Therefore we set ``ping_interval`` to mirror the 30 s idle probe cadence and
keep ``ping_timeout=None`` so unanswered server pings never tear down a
healthy long link.  Incoming client PINGs are answered automatically by the
websockets protocol layer.
"""

import argparse
import asyncio
import atexit
import hashlib
import logging
import os
import re
import socket
import threading
import time
import urllib.parse

import websockets
import websockets.exceptions as _ws_exc

# Match tcurl.c
TCURL_WS_PING_IDLE_SEC = 30
TCURL_WS_PING_PAYLOAD = "tdnotify"

stop_event = None
_SERVER = None        # websockets.server.Server
_LOOP = None          # asyncio.AbstractEventLoop
_THREAD = None        # threading.Thread
_WS_LOG_FILTER_INSTALLED = False
_SHUTDOWN_LOCK = threading.Lock()
_SHUTDOWN_DONE = False

# Active WebSocket connections — force-closed on shutdown so the listening
# socket is released before the next test re-binds the port.
_active_conns = set()


class _WsHandshakeNoiseFilter(logging.Filter):
    """Suppress curl/taosd half-open TCP probes during reconnect tests."""

    _NOISE_MARKERS = (
        "opening handshake failed",
        "connection rejected",
        "did not receive a valid HTTP request",
    )

    def filter(self, record):
        msg = record.getMessage()
        return not any(marker in msg for marker in self._NOISE_MARKERS)


def _install_ws_log_filter():
    global _WS_LOG_FILTER_INSTALLED
    if _WS_LOG_FILTER_INSTALLED:
        return
    filt = _WsHandshakeNoiseFilter()
    for name in ("websockets.server", "websockets"):
        logging.getLogger(name).addFilter(filt)
    _WS_LOG_FILTER_INSTALLED = True


def _ws_exception_handler(loop, context):
    """Suppress expected close-frame and half-open handshake noise."""
    exc = context.get("exception")
    if isinstance(exc, (_ws_exc.ConnectionClosedError,
                        _ws_exc.ConnectionClosedOK,
                        _ws_exc.ConnectionClosed,
                        _ws_exc.InvalidMessage,
                        EOFError)):
        return
    loop.default_exception_handler(context)


def _log_path_for_url(base_dir: str, url_path: str) -> str:
    # url_path example: "/notify?a=1"
    parts = urllib.parse.urlsplit(url_path)
    name = parts.path.strip('/') or 'root'  # "/" -> "root"
    name = name.replace('/', '_')
    name = re.sub(r'[^A-Za-z0-9._-]', '_', name)
    if parts.query:
        h = hashlib.sha1(parts.query.encode('utf-8')).hexdigest()[:8]
        name = f"{name}_{h}"
    return os.path.join(base_dir, f"{name}.log")


def _write_notify_lines(file_path: str, message: str) -> None:
    with open(file_path, "a", encoding="utf-8") as f:
        for line in message.split('\n'):
            line = line.strip()
            if line:
                f.write(line + "\n")
                f.flush()


async def _consume_notify_messages(conn, log_path: str, path: str) -> None:
    """Read TEXT notify payloads; PING/PONG handled by websockets internally."""
    if log_path:
        os.makedirs(log_path, exist_ok=True)
        file_path = _log_path_for_url(log_path, path)
        async for message in conn:
            _write_notify_lines(file_path, message)
            if stop_event is not None and stop_event.is_set():
                break
    else:
        async for message in conn:
            for line in message.split('\n'):
                line = line.strip()
                if line:
                    print(f"Received(no-log)[{path}]: {line}", flush=True)
            if stop_event is not None and stop_event.is_set():
                break


async def handle_websocket(conn, log_path):
    """Handle one persistent taosd notify WebSocket connection."""
    _active_conns.add(conn)
    try:
        raw_path = getattr(conn, "path", None)
        if raw_path is None:
            req = getattr(conn, "request", None)
            raw_path = getattr(req, "path", "/") if req else "/"
        path = raw_path or "/"

        await _consume_notify_messages(conn, log_path, path)
    except (_ws_exc.ConnectionClosedOK,
            _ws_exc.ConnectionClosedError,
            _ws_exc.ConnectionClosed):
        pass
    except Exception as e:
        if stop_event is None or not stop_event.is_set():
            print(f"Connection closed with error: {e}", flush=True)
    finally:
        _active_conns.discard(conn)


async def _close_connection(conn, code=1001):
    try:
        await asyncio.wait_for(conn.close(code), timeout=1.0)
    except Exception:
        transport = getattr(conn, "transport", None)
        if transport is not None and not transport.is_closing():
            transport.abort()


async def _cancel_pending_tasks():
    loop = asyncio.get_running_loop()
    current = asyncio.current_task()
    pending = [t for t in asyncio.all_tasks(loop) if t is not current and not t.done()]
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


async def _start_server(port, log_path):
    global _SERVER
    _SERVER = await websockets.serve(
        lambda c: handle_websocket(c, log_path),
        "0.0.0.0",
        port,
        # Server keepalive PING cadence mirrors tcurl client idle probe.
        ping_interval=TCURL_WS_PING_IDLE_SEC,
        # Never drop the link for a late PONG: taosd only recv-drains on send.
        ping_timeout=None,
        max_size=10 * 1024 * 1024,
        reuse_address=True,
    )
    print(
        f"WebSocket server starts to listen 0.0.0.0:{port} "
        f"(ping_interval={TCURL_WS_PING_IDLE_SEC}s, ping_timeout=disabled)...",
        flush=True,
    )
    return _SERVER


async def _stop_server():
    """Stop server and tear down persistent taosd links."""
    global _SERVER

    if stop_event is not None:
        stop_event.set()

    if _active_conns:
        close_tasks = [
            asyncio.create_task(_close_connection(c, 1001))
            for c in list(_active_conns)
        ]
        if close_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*close_tasks, return_exceptions=True),
                    timeout=2.0,
                )
            except asyncio.TimeoutError:
                for conn in list(_active_conns):
                    transport = getattr(conn, "transport", None)
                    if transport is not None and not transport.is_closing():
                        transport.abort()
        _active_conns.clear()

    if _SERVER is not None:
        _SERVER.close()
        try:
            await asyncio.wait_for(_SERVER.wait_closed(), timeout=3.0)
        except (asyncio.TimeoutError, Exception):
            pass
        _SERVER = None

    await _cancel_pending_tasks()


async def _shutdown_runner(loop):
    global _SHUTDOWN_DONE
    with _SHUTDOWN_LOCK:
        if _SHUTDOWN_DONE:
            return
        _SHUTDOWN_DONE = True

    try:
        await _stop_server()
    except Exception:
        pass
    try:
        await _cancel_pending_tasks()
    except Exception:
        pass


def _reset_shutdown_state():
    global _SHUTDOWN_DONE
    with _SHUTDOWN_LOCK:
        _SHUTDOWN_DONE = False


def _wait_port_free(port: int, timeout: float = 10.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind(("0.0.0.0", port))
                return True
            except OSError:
                time.sleep(0.2)
    return False


def _wait_port_listening(port: int, timeout: float = 5.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.5)
            try:
                if sock.connect_ex(("127.0.0.1", port)) == 0:
                    return True
            except OSError:
                pass
        time.sleep(0.1)
    return False


def wait_notify_server_ready(port: int, timeout: float = 10.0) -> bool:
    """Wait until the notify WS server accepts TCP connections."""
    return _wait_port_listening(port, timeout=timeout)


def start_notify_server_background(port: int, log_path: str):
    """Start the websocket server in a background thread."""
    global _LOOP, _THREAD, stop_event

    if _THREAD is not None and _THREAD.is_alive():
        stop_notify_server_background(timeout=10.0)

    if not _wait_port_free(port, timeout=10.0):
        raise RuntimeError(
            f"port {port} is still in use; previous notify server did not exit"
        )

    stop_event = None
    _reset_shutdown_state()
    _install_ws_log_filter()
    _LOOP = asyncio.new_event_loop()
    _LOOP.set_exception_handler(_ws_exception_handler)

    def _runner():
        global stop_event
        asyncio.set_event_loop(_LOOP)
        stop_event = asyncio.Event()
        try:
            _LOOP.run_until_complete(_start_server(port, log_path))
            _LOOP.run_forever()
        finally:
            try:
                _LOOP.run_until_complete(_shutdown_runner(_LOOP))
            except Exception:
                pass
            try:
                _LOOP.close()
            except Exception:
                pass

    _THREAD = threading.Thread(target=_runner, name="ws-notify-server", daemon=False)
    _THREAD.start()
    if not _wait_port_listening(port, timeout=5.0):
        stop_notify_server_background(timeout=5.0)
        raise RuntimeError(f"notify server failed to listen on port {port}")
    return _LOOP, _THREAD


def stop_notify_server_background(timeout: float = 10.0):
    """Stop the background websocket server and join the thread."""
    global _LOOP, _THREAD, stop_event

    if _LOOP is not None and _LOOP.is_running() and stop_event is not None:
        try:
            _LOOP.call_soon_threadsafe(stop_event.set)
        except Exception:
            pass

    if _LOOP is not None and _LOOP.is_running():
        try:
            fut = asyncio.run_coroutine_threadsafe(_shutdown_runner(_LOOP), _LOOP)
            fut.result(timeout=timeout)
        except Exception:
            print("Failed to stop server cleanly within timeout.", flush=True)
        try:
            _LOOP.call_soon_threadsafe(_LOOP.stop)
        except Exception:
            pass

    if _THREAD is not None:
        _THREAD.join(timeout=timeout)
        if _THREAD.is_alive():
            print("Warning: ws-notify-server thread did not exit in time", flush=True)

    _THREAD = None
    _LOOP = None
    stop_event = None


def _atexit_cleanup():
    stop_notify_server_background(timeout=3.0)


atexit.register(_atexit_cleanup)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--log_path', type=str, default='', help='log file path')
    parser.add_argument('-p', '--port', type=int, default=12345, help='port number')
    args = parser.parse_args()

    async def _main_cli():
        _install_ws_log_filter()
        asyncio.get_running_loop().set_exception_handler(_ws_exception_handler)
        await _start_server(args.port, args.log_path)
        try:
            await asyncio.Future()
        finally:
            await _shutdown_runner(asyncio.get_running_loop())

    asyncio.run(_main_cli())
