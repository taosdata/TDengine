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

from new_test_framework.utils import tdLog, tdSql
import os
import time
import asyncio
import websockets
import argparse
import signal

stop_event = asyncio.Event()

async def handle_websocket(websocket, log_file):
        try:
            # Write the message to the specified log file
            if log_file != "":
                with open(log_file, "a", encoding="utf-8") as f:
                    async for message in websocket:
                        f.write(message + "\n")
                        if stop_event.is_set():
                            break
        except Exception as e:
            print(f"Connection closed with error: {e}")

async def listen(port, log_file):
        async with websockets.serve(
            lambda ws: handle_websocket(ws, log_file),
            "0.0.0.0",
            port,
            ping_timeout = None,
            max_size= 10 * 1024 * 1024 # 10MB,
        ):
            print(f"WebSocket server listening on port {port}...")
            await stop_event.wait()  # Run forever (until canceled)

def signal_handler(sig, frame):
    stop_event.set()

if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--log_file', type=str, default='stream_notify_server.log', help='log file')
    parser.add_argument('-p', '--port', type=int, default=12345, help='port number')
    args = parser.parse_args()

    asyncio.run(listen(args.port, args.log_file))
