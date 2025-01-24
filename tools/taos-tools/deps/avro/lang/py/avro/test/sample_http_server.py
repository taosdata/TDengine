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
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import http.server
import json
from typing import Mapping

import avro.ipc
import avro.protocol

MAIL_PROTOCOL_JSON = json.dumps(
    {
        "namespace": "example.proto",
        "protocol": "Mail",
        "types": [
            {
                "name": "Message",
                "type": "record",
                "fields": [{"name": "to", "type": "string"}, {"name": "from", "type": "string"}, {"name": "body", "type": "string"}],
            }
        ],
        "messages": {
            "send": {"request": [{"name": "message", "type": "Message"}], "response": "string"},
            "replay": {"request": [], "response": "string"},
        },
    }
)
MAIL_PROTOCOL = avro.protocol.parse(MAIL_PROTOCOL_JSON)
SERVER_ADDRESS = ("localhost", 9090)


class MailResponder(avro.ipc.Responder):
    def __init__(self) -> None:
        super().__init__(MAIL_PROTOCOL)

    def invoke(self, message: avro.protocol.Message, request: Mapping[str, Mapping[str, str]]) -> str:
        if message.name == "send":
            return f"Sent message to {request['message']['to']} from {request['message']['from']} with body {request['message']['body']}"
        if message.name == "replay":
            return "replay"
        raise RuntimeError


class MailHandler(http.server.BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        self.responder = MailResponder()
        call_request_reader = avro.ipc.FramedReader(self.rfile)
        call_request = call_request_reader.read_framed_message()
        resp_body = self.responder.respond(call_request)
        self.send_response(200)
        self.send_header("Content-Type", "avro/binary")
        self.end_headers()
        resp_writer = avro.ipc.FramedWriter(self.wfile)
        resp_writer.write_framed_message(resp_body)


def main():
    mail_server = http_server.HTTPServer(SERVER_ADDRESS, MailHandler)
    mail_server.allow_reuse_address = True
    mail_server.serve_forever()


if __name__ == "__main__":
    main()
