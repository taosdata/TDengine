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
require 'uri'
require 'logger'

class GenericResponder < Avro::IPC::Responder
  def initialize(proto, msg, datum)
    proto_json = File.open(proto).read
    super(Avro::Protocol.parse(proto_json))
    @msg = msg
    @datum = datum
  end

  def call(message, _request)
    if message.name == @msg
      STDERR.puts "Message: #{message.name} Datum: #{@datum.inspect}"
      @datum
    end
  end
end

class GenericHandler < WEBrick::HTTPServlet::AbstractServlet
  def do_POST(req, resp)
    call_request = Avro::IPC::FramedReader.new(StringIO.new(req.body)).read_framed_message
    unframed_resp = $responder.respond(call_request)
    writer = Avro::IPC::FramedWriter.new(StringIO.new)
    writer.write_framed_message(unframed_resp)
    resp.body = writer.to_s
  end
end

def run_server(uri, proto, msg, datum)
  uri = URI.parse(uri)
  $responder = GenericResponder.new(proto, msg, datum)
  server = WEBrick::HTTPServer.new(:BindAddress => uri.host,
                                   :Port => uri.port,
                                   :Logger => Logger.new(StringIO.new))
  server.mount '/', GenericHandler
  puts "Port: #{server.config[:Port]}"
  STDOUT.flush
  trap("INT") { server.stop }
  trap("TERM") { server.stop }
  server.start
end

def send_message(uri, proto, msg, datum)
  uri = URI.parse(uri)
  trans = Avro::IPC::HTTPTransceiver.new(uri.host, uri.port)
  proto_json = File.open(proto).read
  requestor = Avro::IPC::Requestor.new(Avro::Protocol.parse(proto_json),
                                       trans)
  p requestor.request(msg, datum)
end

def file_or_stdin(f)
  f == "-" ? STDIN : File.open(f)
end

def main
  if ARGV.size == 0
    puts "Usage: #{$0} [dump|rpcreceive|rpcsend]"
    return 1
  end

  case ARGV[0]
  when "dump"
    if ARGV.size != 3
      puts "Usage: #{$0} dump input_file"
      return 1
    end
    d = Avro::DataFile.new(file_or_stdin(ARGV[1]), Avro::IO::DatumReader.new)
    d.each{|o| puts o.inspect }
    d.close
  when "rpcreceive"
    usage_str = "Usage: #{$0} rpcreceive uri protocol_file "
    usage_str += "message_name (-data d | -file f)"

    unless [4, 6].include?(ARGV.size)
      puts usage_str
      return 1
    end
    uri, proto, msg = ARGV[1,3]
    datum = nil
    if ARGV.size > 4
      case ARGV[4]
      when "-file"
        Avro::DataFile.open(ARGV[5]) do |f|
          datum = f.first
        end
      when "-data"
        puts "JSON Decoder not yet implemented."
        return 1
      else
        puts usage_str
        return 1
      end
    end
    run_server(uri, proto, msg, datum)
  when "rpcsend"
    usage_str = "Usage: #{$0} rpcsend uri protocol_file "
    usage_str += "message_name (-data d | -file f)"
    unless [4,6].include?(ARGV.size)
      puts usage_str
      return 1
    end
    uri, proto, msg = ARGV[1,3]
    datum = nil
    if ARGV.size > 4
      case ARGV[4]
      when "-file"
        Avro::DataFile.open(ARGV[5]){ |f| datum = f.first }
      when "-data"
        puts "JSON Decoder not yet implemented"
        return 1
      else
        puts usage_str
        return 1
      end
    end
    send_message(uri, proto, msg, datum)
  end
  return 0
end

if __FILE__ == $0
  exit(main)
end
