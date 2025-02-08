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
#
# See the License for the specific language governing permissions and
# limitations under the License.
import argparse
import json

import avro.ipc
import avro.protocol

MAIL_PROTOCOL = avro.protocol.parse(
    json.dumps(
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
)
SERVER_HOST = "localhost"
SERVER_PORT = 9090


def make_requestor(server_host: str, server_port: int, protocol: avro.protocol.Protocol) -> avro.ipc.Requestor:
    client = avro.ipc.HTTPTransceiver(server_host, server_port)
    return avro.ipc.Requestor(protocol, client)


def _parse_args() -> argparse.Namespace:
    """Parse the command-line arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument("to", help="Who the message is to")
    parser.add_argument("from_", help="Who the message is from")
    parser.add_argument("body", help="The message body")
    parser.add_argument("num_messages", type=int, default=1, help="The number of messages")
    return parser.parse_args()


def main() -> int:
    # client code - attach to the server and send a message fill in the Message record
    args = _parse_args()
    params = {"message": {"to": args.to, "from": args.from_, "body": args.body}}
    # send the requests and print the result
    for msg_count in range(args.num_messages):
        requestor = make_requestor(SERVER_HOST, SERVER_PORT, MAIL_PROTOCOL)
        result = requestor.request("send", params)
        print(f"Result: {result}")
    # try out a replay message
    requestor = make_requestor(SERVER_HOST, SERVER_PORT, MAIL_PROTOCOL)
    result = requestor.request("replay", dict())
    print(f"Replay Result: {result}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
