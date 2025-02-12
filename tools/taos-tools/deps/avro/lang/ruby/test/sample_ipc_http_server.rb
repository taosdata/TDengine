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
require 'avro'
require 'webrick'

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

class MailResponder < Avro::IPC::Responder
  def initialize
    super(MAIL_PROTOCOL)
  end

  def call(message, request)
    if message.name == 'send'
      request_content = request['message']
      "Sent message to #{request_content['to']} from #{request_content['from']} with body #{request_content['body']}"
    elsif message.name == 'replay'
      'replay'
    end
  end
end

class MailHandler < WEBrick::HTTPServlet::AbstractServlet
  def do_POST(req, resp)
    responder = MailResponder.new
    call_request = Avro::IPC::FramedReader.new(StringIO.new(req.body)).read_framed_message
    unframed_resp = responder.respond(call_request)
    writer = Avro::IPC::FramedWriter.new(StringIO.new)
    writer.write_framed_message(unframed_resp)
    resp.body = writer.to_s
  end
end

if $0 == __FILE__
  server = WEBrick::HTTPServer.new(:Host => 'localhost', :Port => 9090)
  server.mount '/', MailHandler
  trap("INT") { server.shutdown }
  server.start
end
