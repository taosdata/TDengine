#!/usr/bin/env python3

import asyncio
import json
import os
import signal
import time
import websockets

class StreamNotifyServer:
    def __init__(self):
        self.pid = 0
        self.log_file = ""

    def __del__(self):
        self.stop()

    async def handle_websocket(self, websocket):
        try:
            # Write the message to the specified log file
            async for message in websocket:
                if self.log_file != "":
                    with open(self.log_file, "a", encoding="utf-8") as f:
                        f.write(message + "\n")
        except Exception as e:
            print(f"Connection closed with error: {e}")

    async def listen(self, port):
        async with websockets.serve(
            lambda ws: self.handle_websocket(ws),
            "0.0.0.0",
            port
        ):
            print(f"WebSocket server listening on port {port}...")
            await asyncio.Future()  # Run forever (until canceled)

    def run(self, port, log_file):
        self.pid = os.fork()
        self.log_file = log_file
        if self.pid == 0:
            asyncio.run(self.listen(port))
        else:
            print(f"Child process pid is {self.pid}")

    def stop(self):
        if self.pid != 0:
            os.kill(self.pid, signal.SIGTERM)

stop = False

def signal_handler(sig, frame):
    print("Caught signal, exiting gracefully")
    stop = True

if __name__ == "__main__":
    streamNotifyServer = StreamNotifyServer()
    streamNotifyServer.run(12345, "data.log")
    signal.signal(signal.SIGTERM, signal_handler)
    try:
        while not stop:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Caught KeyboardInterrupt, exiting gracefully")
    streamNotifyServer.stop()
