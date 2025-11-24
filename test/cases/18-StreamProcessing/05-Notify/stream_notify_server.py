###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import asyncio
import signal
import websockets
import argparse
import threading
import os
import re
import hashlib
import urllib.parse



# async def handle_connection(websocket, path):
#     async for message in websocket:
#         print(f"收到消息: {message}")
#         await websocket.send(f"服务端回复: {message}")
#
# start_server = websockets.serve(handle_connection, "localhost", 8765)
# asyncio.get_event_loop().run_until_complete(start_server)
# asyncio.get_event_loop().run_forever()

stop_event = asyncio.Event()
_SERVER = None        # websockets.server.Server
_LOOP = None          # asyncio.AbstractEventLoop
_THREAD = None        # threading.Thread

def _log_path_for_url(base_dir: str, url_path: str) -> str:
    # url_path example: "/notify?a=1"
    parts = urllib.parse.urlsplit(url_path)
    name = parts.path.strip('/') or 'root'  # "/" -> "root"
    # collapse nested paths, keep safe chars
    name = name.replace('/', '_')
    name = re.sub(r'[^A-Za-z0-9._-]', '_', name)
    if parts.query:
        h = hashlib.sha1(parts.query.encode('utf-8')).hexdigest()[:8]
        name = f"{name}_{h}"
    return os.path.join(base_dir, f"{name}.log")

async def handle_websocket(conn, log_path):
    try:
        raw_path = getattr(conn, "path", None)
        if raw_path is None:
            req = getattr(conn, "request", None)
            raw_path = getattr(req, "path", "/") if req else "/"
        path = raw_path or "/"

        if log_path:
            os.makedirs(log_path, exist_ok=True)
            file_path = _log_path_for_url(log_path, path)
            #print(f"Logging messages for [{path}] -> {file_path}", flush=True)
            with open(file_path, "a", encoding="utf-8") as f:
                async for message in conn:
                    #print(f"Received[{path}]: {message}", flush=True)
                    f.write(message + "\n")
                    f.flush()
                    if stop_event.is_set():
                        break
        else:
            async for message in conn:
                print(f"Received(no-log)[{path}]: {message}", flush=True)
                if stop_event.is_set():
                    break
    except Exception as e:
        print(f"Connection closed with error: {e}", flush=True)

async def _start_server(port, log_path):
    global _SERVER
    _SERVER = await websockets.serve(
        lambda c: handle_websocket(c, log_path),
        "0.0.0.0",
        port,
        ping_timeout=None,
        max_size=10 * 1024 * 1024,
    )
    print(f"WebSocket server starts to listen 0.0.0.0:{port}...", flush=True)
    return _SERVER

async def _stop_server():
    """
    Gracefully stop server if running.
    """
    global _SERVER
    if _SERVER is not None:
        _SERVER.close()
        try:
            await _SERVER.wait_closed()
        finally:
            _SERVER = None

def start_notify_server_background(port: int, log_path: str):
    """
    Start the websocket server in a background thread.
    Returns (loop, thread).
    """
    global _LOOP, _THREAD
    if _THREAD and _THREAD.is_alive():
        return _LOOP, _THREAD
    _LOOP = asyncio.new_event_loop()
    def _runner():
        asyncio.set_event_loop(_LOOP)
        # start server then run forever until stop_notify_server_background() is called
        _LOOP.run_until_complete(_start_server(port, log_path))
        _LOOP.run_forever()
        # loop stopping path: ensure server closed
        try:
            _LOOP.run_until_complete(_stop_server())
        except Exception:
            pass
    _THREAD = threading.Thread(target=_runner, name="ws-notify-server", daemon=True)
    _THREAD.start()
    return _LOOP, _THREAD

def stop_notify_server_background(timeout: float = 5.0):
    """
    Stop the background websocket server and join the thread.
    """
    global _LOOP, _THREAD
    if not _LOOP:
        return
    # close server within its loop
    try:
        fut = asyncio.run_coroutine_threadsafe(_stop_server(), _LOOP)
        fut.result(timeout=timeout)
    except Exception:
        print("Failed to stop server cleanly within timeout.", flush=True)
        pass
    # stop loop and join
    try:
        _LOOP.call_soon_threadsafe(_LOOP.stop)
    except Exception:
        pass
    if _THREAD:
        _THREAD.join(timeout=timeout)
    _THREAD = None
    _LOOP = None

# def signal_handler(sig, frame):
#     stop_event.set()

if __name__ == '__main__':
    # signal.signal(signal.SIGINT, signal_handler)

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--log_path', type=str, default='', help='log file path')
    parser.add_argument('-p', '--port', type=int, default=12345, help='port number')
    args = parser.parse_args()

    # CLI 模式下前台运行，Ctrl+C 退出
    async def _main_cli():
        await _start_server(args.port, args.log_path)
        try:
            await asyncio.Future()  # run forever
        finally:
            await _stop_server()
    asyncio.run(_main_cli())
