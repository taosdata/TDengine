#!/usr/bin/env python3

##
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Command-line tool

NOTE: The API for the command-line tool is experimental.
"""

import argparse
import http.server
import os.path
import sys
import threading
import urllib.parse
import warnings
from pathlib import Path

import avro.datafile
import avro.io
import avro.ipc
import avro.protocol

server_should_shutdown = False
responder: "GenericResponder"


class GenericResponder(avro.ipc.Responder):
    def __init__(self, proto, msg, datum) -> None:
        avro.ipc.Responder.__init__(self, avro.protocol.parse(Path(proto).read_text()))
        self.msg = msg
        self.datum = datum

    def invoke(self, message, request) -> object:
        global server_should_shutdown
        if message.name != self.msg:
            return None
        print(f"Message: {message.name} Datum: {self.datum}", file=sys.stderr)
        # server will shut down after processing a single Avro request
        server_should_shutdown = True
        return self.datum


class GenericHandler(http.server.BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        self.responder = responder
        call_request_reader = avro.ipc.FramedReader(self.rfile)
        call_request = call_request_reader.read_framed_message()
        resp_body = self.responder.respond(call_request)
        self.send_response(200)
        self.send_header("Content-Type", "avro/binary")
        self.end_headers()
        resp_writer = avro.ipc.FramedWriter(self.wfile)
        resp_writer.write_framed_message(resp_body)
        if server_should_shutdown:
            print("Shutting down server.", file=sys.stderr)
            quitter = threading.Thread(target=self.server.shutdown)
            quitter.daemon = True
            quitter.start()


def run_server(uri: str, proto: str, msg: str, datum: object) -> None:
    global responder
    global server_should_shutdown
    url_obj = urllib.parse.urlparse(uri)
    if url_obj.hostname is None:
        raise RuntimeError(f"uri {uri} must have a hostname.")
    if url_obj.port is None:
        raise RuntimeError(f"uri {uri} must have a port.")
    server_addr = (url_obj.hostname, url_obj.port)
    server_should_shutdown = False
    responder = GenericResponder(proto, msg, datum)
    server = http.server.HTTPServer(server_addr, GenericHandler)
    print(f"Port: {server.server_port}")
    sys.stdout.flush()
    server.allow_reuse_address = True
    print("Starting server.", file=sys.stderr)
    server.serve_forever()


def send_message(uri, proto, msg, datum) -> None:
    url_obj = urllib.parse.urlparse(uri)
    client = avro.ipc.HTTPTransceiver(url_obj.hostname, url_obj.port)
    requestor = avro.ipc.Requestor(avro.protocol.parse(Path(proto).read_text()), client)
    print(requestor.request(msg, datum))


def _parse_args() -> argparse.Namespace:
    """Parse the command-line arguments"""
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(required=True, dest="command") if sys.version_info >= (3, 7) else parser.add_subparsers(dest="command")
    subparser_dump = subparsers.add_parser("dump", help="Dump an avro file")
    subparser_dump.add_argument("input_file", type=argparse.FileType("rb"))
    subparser_rpcreceive = subparsers.add_parser("rpcreceive", help="receive a message")
    subparser_rpcreceive.add_argument("uri")
    subparser_rpcreceive.add_argument("proto")
    subparser_rpcreceive.add_argument("msg")
    subparser_rpcreceive.add_argument("-file", type=argparse.FileType("rb"), required=False)
    subparser_rpcsend = subparsers.add_parser("rpcsend", help="send a message")
    subparser_rpcsend.add_argument("uri")
    subparser_rpcsend.add_argument("proto")
    subparser_rpcsend.add_argument("msg")
    subparser_rpcsend.add_argument("-file", type=argparse.FileType("rb"))
    return parser.parse_args()


def main_dump(args: argparse.Namespace) -> int:
    print("\n".join(f"{d!r}" for d in avro.datafile.DataFileReader(args.input_file, avro.io.DatumReader())))
    return 0


def main_rpcreceive(args: argparse.Namespace) -> int:
    datum = None
    if args.file:
        with avro.datafile.DataFileReader(args.file, avro.io.DatumReader()) as dfr:
            datum = next(dfr)
    run_server(args.uri, args.proto, args.msg, datum)
    return 0


def main_rpcsend(args: argparse.Namespace) -> int:
    datum = None
    if args.file:
        with avro.datafile.DataFileReader(args.file, avro.io.DatumReader()) as dfr:
            datum = next(dfr)
    send_message(args.uri, args.proto, args.msg, datum)
    return 0


def main() -> int:
    args = _parse_args()
    if args.command == "dump":
        return main_dump(args)
    if args.command == "rpcreceive":
        return main_rpcreceive(args)
    if args.command == "rpcsend":
        return main_rpcsend(args)
    return 1


if __name__ == "__main__":
    if os.path.dirname(avro.io.__file__) in sys.path:
        warnings.warn(
            "Invoking avro/tool.py directly is likely to lead to a name collision "
            "with the python io module. Try doing `python -m avro.tool` instead."
        )

    sys.exit(main())
