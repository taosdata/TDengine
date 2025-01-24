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

"""Support for inter-process calls."""

import http.client
import io
import os
import struct

import avro.errors
import avro.io
import avro.protocol
import avro.schema


def _load(name):
    dir_path = os.path.dirname(__file__)
    rsrc_path = os.path.join(dir_path, name)
    with open(rsrc_path) as f:
        return f.read()


HANDSHAKE_REQUEST_SCHEMA_JSON = _load("HandshakeRequest.avsc")
HANDSHAKE_RESPONSE_SCHEMA_JSON = _load("HandshakeResponse.avsc")
HANDSHAKE_REQUEST_SCHEMA = avro.schema.parse(HANDSHAKE_REQUEST_SCHEMA_JSON)
HANDSHAKE_RESPONSE_SCHEMA = avro.schema.parse(HANDSHAKE_RESPONSE_SCHEMA_JSON)

HANDSHAKE_REQUESTOR_WRITER = avro.io.DatumWriter(HANDSHAKE_REQUEST_SCHEMA)
HANDSHAKE_REQUESTOR_READER = avro.io.DatumReader(HANDSHAKE_RESPONSE_SCHEMA)
HANDSHAKE_RESPONDER_WRITER = avro.io.DatumWriter(HANDSHAKE_RESPONSE_SCHEMA)
HANDSHAKE_RESPONDER_READER = avro.io.DatumReader(HANDSHAKE_REQUEST_SCHEMA)

META_SCHEMA = avro.schema.parse('{"type": "map", "values": "bytes"}')
META_WRITER = avro.io.DatumWriter(META_SCHEMA)
META_READER = avro.io.DatumReader(META_SCHEMA)

SYSTEM_ERROR_SCHEMA = avro.schema.parse('["string"]')

# protocol cache
REMOTE_HASHES = {}
REMOTE_PROTOCOLS = {}

BIG_ENDIAN_INT_STRUCT = struct.Struct("!I")
BUFFER_HEADER_LENGTH = 4
BUFFER_SIZE = 8192

#
# Base IPC Classes (Requestor/Responder)
#


class BaseRequestor:
    """Base class for the client side of a protocol interaction."""

    def __init__(self, local_protocol, transceiver):
        self._local_protocol = local_protocol
        self._transceiver = transceiver
        self._remote_protocol = None
        self._remote_hash = None
        self._send_protocol = None

    # read-only properties
    local_protocol = property(lambda self: self._local_protocol)
    transceiver = property(lambda self: self._transceiver)

    # read/write properties
    def set_remote_protocol(self, new_remote_protocol):
        self._remote_protocol = new_remote_protocol
        REMOTE_PROTOCOLS[self.transceiver.remote_name] = self.remote_protocol

    remote_protocol = property(lambda self: self._remote_protocol, set_remote_protocol)

    def set_remote_hash(self, new_remote_hash):
        self._remote_hash = new_remote_hash
        REMOTE_HASHES[self.transceiver.remote_name] = self.remote_hash

    remote_hash = property(lambda self: self._remote_hash, set_remote_hash)

    def set_send_protocol(self, new_send_protocol):
        self._send_protocol = new_send_protocol

    send_protocol = property(lambda self: self._send_protocol, set_send_protocol)

    def request(self, message_name, request_datum):
        """
        Writes a request message and reads a response or error message.
        """
        # build handshake and call request
        buffer_writer = io.BytesIO()
        buffer_encoder = avro.io.BinaryEncoder(buffer_writer)
        self.write_handshake_request(buffer_encoder)
        self.write_call_request(message_name, request_datum, buffer_encoder)

        # send the handshake and call request; block until call response
        call_request = buffer_writer.getvalue()
        return self.issue_request(call_request, message_name, request_datum)

    def write_handshake_request(self, encoder):
        local_hash = self.local_protocol.md5
        remote_name = self.transceiver.remote_name
        remote_hash = REMOTE_HASHES.get(remote_name)
        if remote_hash is None:
            remote_hash = local_hash
            self.remote_protocol = self.local_protocol
        request_datum = {}
        request_datum["clientHash"] = local_hash
        request_datum["serverHash"] = remote_hash
        if self.send_protocol:
            request_datum["clientProtocol"] = str(self.local_protocol)
        HANDSHAKE_REQUESTOR_WRITER.write(request_datum, encoder)

    def write_call_request(self, message_name, request_datum, encoder):
        """
        The format of a call request is:
          * request metadata, a map with values of type bytes
          * the message name, an Avro string, followed by
          * the message parameters. Parameters are serialized according to
            the message's request declaration.
        """
        # request metadata (not yet implemented)
        request_metadata = {}
        META_WRITER.write(request_metadata, encoder)

        # message name
        message = self.local_protocol.messages.get(message_name)
        if message is None:
            raise avro.errors.AvroException(f"Unknown message: {message_name}")
        encoder.write_utf8(message.name)

        # message parameters
        self.write_request(message.request, request_datum, encoder)

    def write_request(self, request_schema, request_datum, encoder):
        datum_writer = avro.io.DatumWriter(request_schema)
        datum_writer.write(request_datum, encoder)

    def read_handshake_response(self, decoder):
        handshake_response = HANDSHAKE_REQUESTOR_READER.read(decoder)
        match = handshake_response.get("match")
        if match == "BOTH":
            self.send_protocol = False
            return True
        elif match == "CLIENT":
            if self.send_protocol:
                raise avro.errors.AvroException("Handshake failure.")
            self.remote_protocol = avro.protocol.parse(handshake_response.get("serverProtocol"))
            self.remote_hash = handshake_response.get("serverHash")
            self.send_protocol = False
            return True
        elif match == "NONE":
            if self.send_protocol:
                raise avro.errors.AvroException("Handshake failure.")
            self.remote_protocol = avro.protocol.parse(handshake_response.get("serverProtocol"))
            self.remote_hash = handshake_response.get("serverHash")
            self.send_protocol = True
            return False
        else:
            raise avro.errors.AvroException(f"Unexpected match: {match}")

    def read_call_response(self, message_name, decoder):
        """
        The format of a call response is:
          * response metadata, a map with values of type bytes
          * a one-byte error flag boolean, followed by either:
            o if the error flag is false,
              the message response, serialized per the message's response schema.
            o if the error flag is true,
              the error, serialized per the message's error union schema.
        """
        # response metadata
        response_metadata = META_READER.read(decoder)

        # remote response schema
        remote_message_schema = self.remote_protocol.messages.get(message_name)
        if remote_message_schema is None:
            raise avro.errors.AvroException(f"Unknown remote message: {message_name}")

        # local response schema
        local_message_schema = self.local_protocol.messages.get(message_name)
        if local_message_schema is None:
            raise avro.errors.AvroException(f"Unknown local message: {message_name}")

        # error flag
        if not decoder.read_boolean():
            writers_schema = remote_message_schema.response
            readers_schema = local_message_schema.response
            return self.read_response(writers_schema, readers_schema, decoder)
        else:
            writers_schema = remote_message_schema.errors
            readers_schema = local_message_schema.errors
            datum_reader = avro.io.DatumReader(writers_schema, readers_schema)
            raise avro.errors.AvroRemoteException(datum_reader.read(decoder))

    def read_response(self, writers_schema, readers_schema, decoder):
        datum_reader = avro.io.DatumReader(writers_schema, readers_schema)
        result = datum_reader.read(decoder)
        return result


class Requestor(BaseRequestor):
    def issue_request(self, call_request, message_name, request_datum):
        call_response = self.transceiver.transceive(call_request)

        # process the handshake and call response
        buffer_decoder = avro.io.BinaryDecoder(io.BytesIO(call_response))
        call_response_exists = self.read_handshake_response(buffer_decoder)
        if call_response_exists:
            return self.read_call_response(message_name, buffer_decoder)
        return self.request(message_name, request_datum)


class Responder:
    """Base class for the server side of a protocol interaction."""

    def __init__(self, local_protocol):
        self._local_protocol = local_protocol
        self._local_hash = self.local_protocol.md5
        self._protocol_cache = {}
        self.set_protocol_cache(self.local_hash, self.local_protocol)

    # read-only properties
    local_protocol = property(lambda self: self._local_protocol)
    local_hash = property(lambda self: self._local_hash)
    protocol_cache = property(lambda self: self._protocol_cache)

    # utility functions to manipulate protocol cache
    def get_protocol_cache(self, hash):
        return self.protocol_cache.get(hash)

    def set_protocol_cache(self, hash, protocol):
        self.protocol_cache[hash] = protocol

    def respond(self, call_request):
        """
        Called by a server to deserialize a request, compute and serialize
        a response or error. Compare to 'handle()' in Thrift.
        """
        buffer_reader = io.BytesIO(call_request)
        buffer_decoder = avro.io.BinaryDecoder(buffer_reader)
        buffer_writer = io.BytesIO()
        buffer_encoder = avro.io.BinaryEncoder(buffer_writer)
        error = None
        response_metadata = {}

        try:
            remote_protocol = self.process_handshake(buffer_decoder, buffer_encoder)
            # handshake failure
            if remote_protocol is None:
                return buffer_writer.getvalue()

            # read request using remote protocol
            request_metadata = META_READER.read(buffer_decoder)
            remote_message_name = buffer_decoder.read_utf8()

            # get remote and local request schemas so we can do
            # schema resolution (one fine day)
            remote_message = remote_protocol.messages.get(remote_message_name)
            if remote_message is None:
                fail_msg = f"Unknown remote message: {remote_message_name}"
                raise avro.errors.AvroException(fail_msg)
            local_message = self.local_protocol.messages.get(remote_message_name)
            if local_message is None:
                fail_msg = f"Unknown local message: {remote_message_name}"
                raise avro.errors.AvroException(fail_msg)
            writers_schema = remote_message.request
            readers_schema = local_message.request
            request = self.read_request(writers_schema, readers_schema, buffer_decoder)

            # perform server logic
            try:
                response = self.invoke(local_message, request)
            except avro.errors.AvroRemoteException as e:
                error = e
            except Exception as e:
                error = avro.errors.AvroRemoteException(str(e))

            # write response using local protocol
            META_WRITER.write(response_metadata, buffer_encoder)
            buffer_encoder.write_boolean(error is not None)
            if error is None:
                writers_schema = local_message.response
                self.write_response(writers_schema, response, buffer_encoder)
            else:
                writers_schema = local_message.errors
                self.write_error(writers_schema, error, buffer_encoder)
        except schema.AvroException as e:
            error = avro.errors.AvroRemoteException(str(e))
            buffer_encoder = avro.io.BinaryEncoder(io.BytesIO())
            META_WRITER.write(response_metadata, buffer_encoder)
            buffer_encoder.write_boolean(True)
            self.write_error(SYSTEM_ERROR_SCHEMA, error, buffer_encoder)
        return buffer_writer.getvalue()

    def process_handshake(self, decoder, encoder):
        handshake_request = HANDSHAKE_RESPONDER_READER.read(decoder)
        handshake_response = {}

        # determine the remote protocol
        client_hash = handshake_request.get("clientHash")
        client_protocol = handshake_request.get("clientProtocol")
        remote_protocol = self.get_protocol_cache(client_hash)
        if remote_protocol is None and client_protocol is not None:
            remote_protocol = avro.protocol.parse(client_protocol)
            self.set_protocol_cache(client_hash, remote_protocol)

        # evaluate remote's guess of the local protocol
        server_hash = handshake_request.get("serverHash")
        if self.local_hash == server_hash:
            if remote_protocol is None:
                handshake_response["match"] = "NONE"
            else:
                handshake_response["match"] = "BOTH"
        else:
            if remote_protocol is None:
                handshake_response["match"] = "NONE"
            else:
                handshake_response["match"] = "CLIENT"

        if handshake_response["match"] != "BOTH":
            handshake_response["serverProtocol"] = str(self.local_protocol)
            handshake_response["serverHash"] = self.local_hash

        HANDSHAKE_RESPONDER_WRITER.write(handshake_response, encoder)
        return remote_protocol

    def invoke(self, local_message, request):
        """
        Aactual work done by server: cf. handler in thrift.
        """
        pass

    def read_request(self, writers_schema, readers_schema, decoder):
        datum_reader = avro.io.DatumReader(writers_schema, readers_schema)
        return datum_reader.read(decoder)

    def write_response(self, writers_schema, response_datum, encoder):
        datum_writer = avro.io.DatumWriter(writers_schema)
        datum_writer.write(response_datum, encoder)

    def write_error(self, writers_schema, error_exception, encoder):
        datum_writer = avro.io.DatumWriter(writers_schema)
        datum_writer.write(str(error_exception), encoder)


#
# Utility classes
#


class FramedReader:
    """Wrapper around a file-like object to read framed data."""

    def __init__(self, reader):
        self._reader = reader

    # read-only properties
    reader = property(lambda self: self._reader)

    def read_framed_message(self):
        message = []
        while True:
            buffer = io.BytesIO()
            buffer_length = self._read_buffer_length()
            if buffer_length == 0:
                return b"".join(message)
            while buffer.tell() < buffer_length:
                chunk = self.reader.read(buffer_length - buffer.tell())
                if chunk == "":
                    raise avro.errors.ConnectionClosedException("Reader read 0 bytes.")
                buffer.write(chunk)
            message.append(buffer.getvalue())

    def _read_buffer_length(self):
        read = self.reader.read(BUFFER_HEADER_LENGTH)
        if read == "":
            raise avro.errors.ConnectionClosedException("Reader read 0 bytes.")
        return BIG_ENDIAN_INT_STRUCT.unpack(read)[0]


class FramedWriter:
    """Wrapper around a file-like object to write framed data."""

    def __init__(self, writer):
        self._writer = writer

    # read-only properties
    writer = property(lambda self: self._writer)

    def write_framed_message(self, message):
        message_length = len(message)
        total_bytes_sent = 0
        while message_length - total_bytes_sent > 0:
            if message_length - total_bytes_sent > BUFFER_SIZE:
                buffer_length = BUFFER_SIZE
            else:
                buffer_length = message_length - total_bytes_sent
            self.write_buffer(message[total_bytes_sent : (total_bytes_sent + buffer_length)])
            total_bytes_sent += buffer_length
        # A message is always terminated by a zero-length buffer.
        self.write_buffer_length(0)

    def write_buffer(self, chunk):
        buffer_length = len(chunk)
        self.write_buffer_length(buffer_length)
        self.writer.write(chunk)

    def write_buffer_length(self, n):
        self.writer.write(BIG_ENDIAN_INT_STRUCT.pack(n))


#
# Transceiver Implementations
#


class HTTPTransceiver:
    """
    A simple HTTP-based transceiver implementation.
    Useful for clients but not for servers
    """

    def __init__(self, host, port, req_resource="/"):
        self.req_resource = req_resource
        self.conn = http.client.HTTPConnection(host, port)
        self.conn.connect()
        self.remote_name = self.conn.sock.getsockname()

    def transceive(self, request):
        self.write_framed_message(request)
        result = self.read_framed_message()
        return result

    def read_framed_message(self):
        response = self.conn.getresponse()
        response_reader = FramedReader(response)
        framed_message = response_reader.read_framed_message()
        response.read()  # ensure we're ready for subsequent requests
        return framed_message

    def write_framed_message(self, message):
        req_method = "POST"
        req_headers = {"Content-Type": "avro/binary"}

        req_body_buffer = FramedWriter(io.BytesIO())
        req_body_buffer.write_framed_message(message)
        req_body = req_body_buffer.writer.getvalue()

        self.conn.request(req_method, self.req_resource, req_body, req_headers)

    def close(self):
        self.conn.close()


#
# Server Implementations (none yet)
#
