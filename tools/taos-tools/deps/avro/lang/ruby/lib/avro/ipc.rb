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

require "net/http"

module Avro::IPC

  class AvroRemoteError < Avro::AvroError; end

  HANDSHAKE_REQUEST_SCHEMA = Avro::Schema.parse <<-JSON
  {
    "type": "record",
    "name": "HandshakeRequest", "namespace":"org.apache.avro.ipc",
    "fields": [
      {"name": "clientHash",
       "type": {"type": "fixed", "name": "MD5", "size": 16}},
      {"name": "clientProtocol", "type": ["null", "string"]},
      {"name": "serverHash", "type": "MD5"},
      {"name": "meta", "type": ["null", {"type": "map", "values": "bytes"}]}
    ]
  }
  JSON

  HANDSHAKE_RESPONSE_SCHEMA = Avro::Schema.parse <<-JSON
  {
    "type": "record",
    "name": "HandshakeResponse", "namespace": "org.apache.avro.ipc",
    "fields": [
      {"name": "match",
       "type": {"type": "enum", "name": "HandshakeMatch",
                "symbols": ["BOTH", "CLIENT", "NONE"]}},
      {"name": "serverProtocol", "type": ["null", "string"]},
      {"name": "serverHash",
       "type": ["null", {"type": "fixed", "name": "MD5", "size": 16}]},
      {"name": "meta",
       "type": ["null", {"type": "map", "values": "bytes"}]}
    ]
  }
  JSON

  HANDSHAKE_REQUESTOR_WRITER = Avro::IO::DatumWriter.new(HANDSHAKE_REQUEST_SCHEMA)
  HANDSHAKE_REQUESTOR_READER = Avro::IO::DatumReader.new(HANDSHAKE_RESPONSE_SCHEMA)
  HANDSHAKE_RESPONDER_WRITER = Avro::IO::DatumWriter.new(HANDSHAKE_RESPONSE_SCHEMA)
  HANDSHAKE_RESPONDER_READER = Avro::IO::DatumReader.new(HANDSHAKE_REQUEST_SCHEMA)

  META_SCHEMA = Avro::Schema.parse('{"type": "map", "values": "bytes"}')
  META_WRITER = Avro::IO::DatumWriter.new(META_SCHEMA)
  META_READER = Avro::IO::DatumReader.new(META_SCHEMA)

  SYSTEM_ERROR_SCHEMA = Avro::Schema.parse('["string"]')

  # protocol cache
  # rubocop:disable Style/MutableConstant
  REMOTE_HASHES = {}
  REMOTE_PROTOCOLS = {}
  # rubocop:enable Style/MutableConstant


  BUFFER_HEADER_LENGTH = 4
  BUFFER_SIZE = 8192

  # Raised when an error message is sent by an Avro requestor or responder.
  class AvroRemoteException < Avro::AvroError; end

  class ConnectionClosedException < Avro::AvroError; end

  # Base class for the client side of a protocol interaction.
  class Requestor
    attr_reader :local_protocol, :transport, :remote_protocol, :remote_hash
    attr_accessor :send_protocol

    def initialize(local_protocol, transport)
      @local_protocol = local_protocol
      @transport = transport
      @remote_protocol = nil
      @remote_hash = nil
      @send_protocol = nil
    end

    def remote_protocol=(new_remote_protocol)
      @remote_protocol = new_remote_protocol
      REMOTE_PROTOCOLS[transport.remote_name] = remote_protocol
    end

    def remote_hash=(new_remote_hash)
      @remote_hash = new_remote_hash
      REMOTE_HASHES[transport.remote_name] = remote_hash
    end

    def request(message_name, request_datum)
      # Writes a request message and reads a response or error message.
      # build handshake and call request
      buffer_writer = StringIO.new(String.new('', encoding: 'BINARY'))
      buffer_encoder = Avro::IO::BinaryEncoder.new(buffer_writer)
      write_handshake_request(buffer_encoder)
      write_call_request(message_name, request_datum, buffer_encoder)

      # send the handshake and call request;  block until call response
      call_request = buffer_writer.string
      call_response = transport.transceive(call_request)

      # process the handshake and call response
      buffer_decoder = Avro::IO::BinaryDecoder.new(StringIO.new(call_response))
      if read_handshake_response(buffer_decoder)
        read_call_response(message_name, buffer_decoder)
      else
        request(message_name, request_datum)
      end
    end

    def write_handshake_request(encoder)
      local_hash = local_protocol.md5
      remote_name = transport.remote_name
      remote_hash = REMOTE_HASHES[remote_name]
      unless remote_hash
        remote_hash = local_hash
        self.remote_protocol = local_protocol
      end
      request_datum = {
        'clientHash' => local_hash,
        'serverHash' => remote_hash
      }
      if send_protocol
        request_datum['clientProtocol'] = local_protocol.to_s
      end
      HANDSHAKE_REQUESTOR_WRITER.write(request_datum, encoder)
    end

    def write_call_request(message_name, request_datum, encoder)
      # The format of a call request is:
      #   * request metadata, a map with values of type bytes
      #   * the message name, an Avro string, followed by
      #   * the message parameters. Parameters are serialized according to
      #     the message's request declaration.

      # TODO request metadata (not yet implemented)
      request_metadata = {}
      META_WRITER.write(request_metadata, encoder)

      message = local_protocol.messages[message_name]
      unless message
        raise AvroError, "Unknown message: #{message_name}"
      end
      encoder.write_string(message.name)

      write_request(message.request, request_datum, encoder)
    end

    def write_request(request_schema, request_datum, encoder)
      datum_writer = Avro::IO::DatumWriter.new(request_schema)
      datum_writer.write(request_datum, encoder)
    end

    def read_handshake_response(decoder)
      handshake_response = HANDSHAKE_REQUESTOR_READER.read(decoder)
      we_have_matching_schema = false

      case handshake_response['match']
      when 'BOTH'
        self.send_protocol = false
        we_have_matching_schema = true
      when 'CLIENT'
        raise AvroError.new('Handshake failure. match == CLIENT') if send_protocol
        self.remote_protocol = Avro::Protocol.parse(handshake_response['serverProtocol'])
        self.remote_hash = handshake_response['serverHash']
        self.send_protocol = false
        we_have_matching_schema = true
      when 'NONE'
        raise AvroError.new('Handshake failure. match == NONE') if send_protocol
        self.remote_protocol = Avro::Protocol.parse(handshake_response['serverProtocol'])
        self.remote_hash = handshake_response['serverHash']
        self.send_protocol = true
      else
        raise AvroError.new("Unexpected match: #{match}")
      end

      return we_have_matching_schema
    end

    def read_call_response(message_name, decoder)
      # The format of a call response is:
      #   * response metadata, a map with values of type bytes
      #   * a one-byte error flag boolean, followed by either:
      #     * if the error flag is false,
      #       the message response, serialized per the message's response schema.
      #     * if the error flag is true,
      #       the error, serialized per the message's error union schema.
      _response_metadata = META_READER.read(decoder)

      # remote response schema
      remote_message_schema = remote_protocol.messages[message_name]
      raise AvroError.new("Unknown remote message: #{message_name}") unless remote_message_schema

      # local response schema
      local_message_schema = local_protocol.messages[message_name]
      unless local_message_schema
        raise AvroError.new("Unknown local message: #{message_name}")
      end

      # error flag
      if !decoder.read_boolean
        writers_schema = remote_message_schema.response
        readers_schema = local_message_schema.response
        read_response(writers_schema, readers_schema, decoder)
      else
        writers_schema = remote_message_schema.errors || SYSTEM_ERROR_SCHEMA
        readers_schema = local_message_schema.errors || SYSTEM_ERROR_SCHEMA
        raise read_error(writers_schema, readers_schema, decoder)
      end
    end

    def read_response(writers_schema, readers_schema, decoder)
      datum_reader = Avro::IO::DatumReader.new(writers_schema, readers_schema)
      datum_reader.read(decoder)
    end

    def read_error(writers_schema, readers_schema, decoder)
      datum_reader = Avro::IO::DatumReader.new(writers_schema, readers_schema)
      AvroRemoteError.new(datum_reader.read(decoder))
    end
  end

  # Base class for the server side of a protocol interaction.
  class Responder
    attr_reader :local_protocol, :local_hash, :protocol_cache
    def initialize(local_protocol)
      @local_protocol = local_protocol
      @local_hash = self.local_protocol.md5
      @protocol_cache = {}
      protocol_cache[local_hash] = local_protocol
    end

    # Called by a server to deserialize a request, compute and serialize
    # a response or error. Compare to 'handle()' in Thrift.
    def respond(call_request, transport=nil)
      buffer_decoder = Avro::IO::BinaryDecoder.new(StringIO.new(call_request))
      buffer_writer = StringIO.new(String.new('', encoding: 'BINARY'))
      buffer_encoder = Avro::IO::BinaryEncoder.new(buffer_writer)
      error = nil
      response_metadata = {}

      begin
        remote_protocol = process_handshake(buffer_decoder, buffer_encoder, transport)
        # handshake failure
        unless remote_protocol
          return buffer_writer.string
        end

        # read request using remote protocol
        _request_metadata = META_READER.read(buffer_decoder)
        remote_message_name = buffer_decoder.read_string

        # get remote and local request schemas so we can do
        # schema resolution (one fine day)
        remote_message = remote_protocol.messages[remote_message_name]
        unless remote_message
          raise AvroError.new("Unknown remote message: #{remote_message_name}")
        end
        local_message = local_protocol.messages[remote_message_name]
        unless local_message
          raise AvroError.new("Unknown local message: #{remote_message_name}")
        end
        writers_schema = remote_message.request
        readers_schema = local_message.request
        request = read_request(writers_schema, readers_schema, buffer_decoder)
        # perform server logic
        begin
          response = call(local_message, request)
        rescue AvroRemoteError => e
          error = e
        rescue Exception => e # rubocop:disable Lint/RescueException
          error = AvroRemoteError.new(e.to_s)
        end

        # write response using local protocol
        META_WRITER.write(response_metadata, buffer_encoder)
        buffer_encoder.write_boolean(!!error)
        if error.nil?
          writers_schema = local_message.response
          write_response(writers_schema, response, buffer_encoder)
        else
          writers_schema = local_message.errors || SYSTEM_ERROR_SCHEMA
          write_error(writers_schema, error, buffer_encoder)
        end
      rescue Avro::AvroError => e
        error = AvroRemoteException.new(e.to_s)
        # TODO does the stuff written here ever get used?
        buffer_encoder = Avro::IO::BinaryEncoder.new(StringIO.new)
        META_WRITER.write(response_metadata, buffer_encoder)
        buffer_encoder.write_boolean(true)
        self.write_error(SYSTEM_ERROR_SCHEMA, error, buffer_encoder)
      end
      buffer_writer.string
    end

    def process_handshake(decoder, encoder, connection=nil)
      if connection && connection.is_connected?
        return connection.protocol
      end
      handshake_request = HANDSHAKE_RESPONDER_READER.read(decoder)
      handshake_response = {}

      # determine the remote protocol
      client_hash = handshake_request['clientHash']
      client_protocol = handshake_request['clientProtocol']
      remote_protocol = protocol_cache[client_hash]

      if !remote_protocol && client_protocol
        remote_protocol = Avro::Protocol.parse(client_protocol)
        protocol_cache[client_hash] = remote_protocol
      end

      # evaluate remote's guess of the local protocol
      server_hash = handshake_request['serverHash']
      if local_hash == server_hash
        if !remote_protocol
          handshake_response['match'] = 'NONE'
        else
          handshake_response['match'] = 'BOTH'
        end
      else
        if !remote_protocol
          handshake_response['match'] = 'NONE'
        else
          handshake_response['match'] = 'CLIENT'
        end
      end

      if handshake_response['match'] != 'BOTH'
        handshake_response['serverProtocol'] = local_protocol.to_s
        handshake_response['serverHash'] = local_hash
      end

      HANDSHAKE_RESPONDER_WRITER.write(handshake_response, encoder)

      if connection && handshake_response['match'] != 'NONE'
        connection.protocol = remote_protocol
      end

      remote_protocol
    end

    def call(_local_message, _request)
      # Actual work done by server: cf. handler in thrift.
      raise NotImplementedError
    end

    def read_request(writers_schema, readers_schema, decoder)
      datum_reader = Avro::IO::DatumReader.new(writers_schema, readers_schema)
      datum_reader.read(decoder)
    end

    def write_response(writers_schema, response_datum, encoder)
      datum_writer = Avro::IO::DatumWriter.new(writers_schema)
      datum_writer.write(response_datum, encoder)
    end

    def write_error(writers_schema, error_exception, encoder)
      datum_writer = Avro::IO::DatumWriter.new(writers_schema)
      datum_writer.write(error_exception.to_s, encoder)
    end
  end

  class SocketTransport
    # A simple socket-based Transport implementation.

    attr_reader :sock, :remote_name
    attr_accessor :protocol

    def initialize(sock)
      @sock = sock
      @protocol = nil
    end

    def is_connected?()
      !!@protocol
    end

    def transceive(request)
      write_framed_message(request)
      read_framed_message
    end

    def read_framed_message
      message = []
      loop do
        buffer = StringIO.new(String.new('', encoding: 'BINARY'))
        buffer_length = read_buffer_length
        if buffer_length == 0
          return message.join
        end
        while buffer.tell < buffer_length
          chunk = sock.read(buffer_length - buffer.tell)
          if chunk == ''
            raise ConnectionClosedException.new("Socket read 0 bytes.")
          end
          buffer.write(chunk)
        end
        message << buffer.string
      end
    end

    def write_framed_message(message)
      message_length = message.bytesize
      total_bytes_sent = 0
      while message_length - total_bytes_sent > 0
        if message_length - total_bytes_sent > BUFFER_SIZE
          buffer_length = BUFFER_SIZE
        else
          buffer_length = message_length - total_bytes_sent
        end
        write_buffer(message[total_bytes_sent,buffer_length])
        total_bytes_sent += buffer_length
      end
      # A message is always terminated by a zero-length buffer.
      write_buffer_length(0)
    end

    def write_buffer(chunk)
      buffer_length = chunk.bytesize
      write_buffer_length(buffer_length)
      total_bytes_sent = 0
      while total_bytes_sent < buffer_length
        bytes_sent = self.sock.write(chunk[total_bytes_sent..-1])
        if bytes_sent == 0
          raise ConnectionClosedException.new("Socket sent 0 bytes.")
        end
        total_bytes_sent += bytes_sent
      end
    end

    def write_buffer_length(n)
      bytes_sent = sock.write([n].pack('N'))
      if bytes_sent == 0
        raise ConnectionClosedException.new("socket sent 0 bytes")
      end
    end

    def read_buffer_length
      read = sock.read(BUFFER_HEADER_LENGTH)
      if read == '' || read == nil
        raise ConnectionClosedException.new("Socket read 0 bytes.")
      end
      read.unpack('N')[0]
    end

    def close
      sock.close
    end
  end

  class ConnectionClosedError < StandardError; end

  class FramedWriter
    attr_reader :writer
    def initialize(writer)
      @writer = writer
    end

    def write_framed_message(message)
      message_size = message.bytesize
      total_bytes_sent = 0
      while message_size - total_bytes_sent > 0
        if message_size - total_bytes_sent > BUFFER_SIZE
          buffer_size = BUFFER_SIZE
        else
          buffer_size = message_size - total_bytes_sent
        end
        write_buffer(message[total_bytes_sent, buffer_size])
        total_bytes_sent += buffer_size
      end
      write_buffer_size(0)
    end

    def to_s; writer.string; end

    private
    def write_buffer(chunk)
      buffer_size = chunk.bytesize
      write_buffer_size(buffer_size)
      writer << chunk
    end

    def write_buffer_size(n)
      writer.write([n].pack('N'))
    end
  end

  class FramedReader
    attr_reader :reader

    def initialize(reader)
      @reader = reader
    end

    def read_framed_message
      message = []
      loop do
        buffer = String.new('', encoding: 'BINARY')
        buffer_size = read_buffer_size

        return message.join if buffer_size == 0

        while buffer.bytesize < buffer_size
          chunk = reader.read(buffer_size - buffer.bytesize)
          chunk_error?(chunk)
          buffer << chunk
        end
        message << buffer
      end
    end

    private
    def read_buffer_size
      header = reader.read(BUFFER_HEADER_LENGTH)
      chunk_error?(header)
      header.unpack('N')[0]
    end

    def chunk_error?(chunk)
      raise ConnectionClosedError.new("Reader read 0 bytes") if chunk == ''
    end
  end

  # Only works for clients. Sigh.
  class HTTPTransceiver
    attr_reader :remote_name, :host, :port
    def initialize(host, port)
      @host, @port = host, port
      @remote_name = "#{host}:#{port}"
      @conn = Net::HTTP.start host, port
    end

    def transceive(message)
      writer = FramedWriter.new(StringIO.new(String.new('', encoding: 'BINARY')))
      writer.write_framed_message(message)
      resp = @conn.post('/', writer.to_s, {'Content-Type' => 'avro/binary'})
      FramedReader.new(StringIO.new(resp.body)).read_framed_message
    end
  end
end
