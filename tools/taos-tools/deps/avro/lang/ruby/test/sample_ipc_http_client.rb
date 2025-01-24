#!/usr/bin/env ruby
# frozen_string_literal: true
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

require 'socket'
require 'avro'

MAIL_PROTOCOL_JSON = <<-JSON
{"namespace": "example.proto",
 "protocol": "Mail",

 "types": [
     {"name": "Message", "type": "record",
      "fields": [
          {"name": "to",   "type": "string"},
          {"name": "from", "type": "string"},
          {"name": "body", "type": "string"}
      ]
     }
 ],

 "messages": {
     "send": {
         "request": [{"name": "message", "type": "Message"}],
         "response": "string"
     },
     "replay": {
         "request": [],
         "response": "string"
     }
 }
}
JSON

MAIL_PROTOCOL = Avro::Protocol.parse(MAIL_PROTOCOL_JSON)

def make_requestor(server_address, port, protocol)
  transport = Avro::IPC::HTTPTransceiver.new(server_address, port)
  Avro::IPC::Requestor.new(protocol, transport)
end

if $0 == __FILE__
  if ![3, 4].include?(ARGV.length)
    raise "Usage: <to> <from> <body> [<count>]"
  end

  # client code - attach to the server and send a message
  # fill in the Message record
  message = {
    'to'   => ARGV[0],
    'from' => ARGV[1],
    'body' => ARGV[2]
  }

  num_messages = (ARGV[3] || 1).to_i

  # build the parameters for the request
  params = {'message' => message}
  # send the requests and print the result

  num_messages.times do
    requestor = make_requestor('localhost', 9090, MAIL_PROTOCOL)
    result = requestor.request('send', params)
    puts("Result: " + result)
  end

  # try out a replay message
  requestor = make_requestor('localhost', 9090, MAIL_PROTOCOL)
  result = requestor.request('replay', {})
  puts("Replay Result: " + result)
end
