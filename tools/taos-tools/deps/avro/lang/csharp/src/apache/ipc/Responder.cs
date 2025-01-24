/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.IO;
using Avro.Generic;
using Avro.IO;
using Avro.Specific;
using log4net;
using org.apache.avro.ipc;

namespace Avro.ipc
{
    public abstract class Responder
    {
        private static readonly ILog log = LogManager.GetLogger(typeof (Responder));

        private static readonly Schema META = MapSchema.CreateMap(PrimitiveSchema.NewInstance("bytes"));

        private static readonly GenericReader<Dictionary<String, object>>
            META_READER = new GenericReader<Dictionary<String, object>>(META, META);

        private static readonly GenericWriter<IDictionary<String, object>>
            META_WRITER = new GenericWriter<IDictionary<String, object>>(META);

        private readonly SpecificReader<HandshakeRequest> handshakeReader =
            new SpecificReader<HandshakeRequest>(new HandshakeRequest().Schema, new HandshakeRequest().Schema);

        private readonly SpecificWriter<HandshakeResponse> handshakeWriter =
            new SpecificWriter<HandshakeResponse>(new HandshakeResponse().Schema);

        private readonly Protocol local;
        private readonly MD5 localHash;
        private readonly IDictionary<Schema, Protocol> protocols = new Dictionary<Schema, Protocol>();
        private readonly object protocolsLock = new object();

        protected Responder(Protocol local)
        {
            this.local = local;
            localHash = new MD5 {Value = local.MD5};

            lock (protocolsLock)
            {
                protocols[localHash.Schema] = local;
            }
        }

        public Protocol Local
        {
            get { return local; }
        }

        public abstract Object Respond(Message message, Object request);
        public abstract Object ReadRequest(Schema actual, Schema expected, Decoder input);

        public abstract void WriteResponse(Schema schema, Object response, Encoder output);
        public abstract void WriteError(Schema schema, Object error, Encoder output);

        public IList<MemoryStream> Respond(IList<MemoryStream> buffers)
        {
            return Respond(buffers, null);
        }

        private Protocol Handshake(Decoder input, Encoder output, Transceiver connection)
        {
            if (connection != null && connection.IsConnected)
                return connection.Remote;
            HandshakeRequest request = handshakeReader.Read(null, input);

            Protocol remote;
            lock (protocolsLock)
            {
                remote = protocols[request.clientHash.Schema];
                if (remote == null && request.clientProtocol != null)
                {
                    remote = Protocol.Parse(request.clientProtocol);
                    protocols[request.clientHash.Schema] = remote;
                }
            }
            var response = new HandshakeResponse();
            if (localHash.Schema.Equals(request.serverHash.Schema))
            {
                response.match =
                    remote == null ? HandshakeMatch.NONE : HandshakeMatch.BOTH;
            }
            else
            {
                response.match =
                    remote == null ? HandshakeMatch.NONE : HandshakeMatch.CLIENT;
            }
            if (response.match != HandshakeMatch.BOTH)
            {
                response.serverProtocol = local.ToString();
                response.serverHash = localHash;
            }

            handshakeWriter.Write(response, output);

            if (connection != null && response.match != HandshakeMatch.NONE)
                connection.Remote = remote;

            return remote;
        }

        public IList<MemoryStream> Respond(IList<MemoryStream> buffers,
                                          Transceiver connection)
        {
            Decoder input = new BinaryDecoder(new ByteBufferInputStream(buffers));

            var bbo = new ByteBufferOutputStream();
            var output = new BinaryEncoder(bbo);
            Exception error = null;
            var context = new RpcContext();
            List<MemoryStream> handshake = null;

            bool wasConnected = connection != null && connection.IsConnected;
            try
            {
                Protocol remote = Handshake(input, output, connection);
                output.Flush();
                if (remote == null) // handshake failed
                    return bbo.GetBufferList();
                handshake = bbo.GetBufferList();

                // read request using remote protocol specification
                context.RequestCallMeta = META_READER.Read(null, input);
                String messageName = input.ReadString();
                if (messageName.Equals("")) // a handshake ping
                    return handshake;
                Message rm = remote.Messages[messageName];
                if (rm == null)
                    throw new AvroRuntimeException("No such remote message: " + messageName);
                Message m = Local.Messages[messageName];
                if (m == null)
                    throw new AvroRuntimeException("No message named " + messageName
                                                   + " in " + Local);

                Object request = ReadRequest(rm.Request, m.Request, input);

                context.Message = rm;

                // create response using local protocol specification
                if ((m.Oneway.GetValueOrDefault() != rm.Oneway.GetValueOrDefault()) && wasConnected)
                    throw new AvroRuntimeException("Not both one-way: " + messageName);

                Object response = null;

                try
                {
                    response = Respond(m, request);
                    context.Response = response;
                }
                catch (Exception e)
                {
                    error = e;
                    context.Error = error;
                    log.Warn("user error", e);
                }

                if (m.Oneway.GetValueOrDefault() && wasConnected) // no response data
                    return null;

                output.WriteBoolean(error != null);
                if (error == null)
                    WriteResponse(m.Response, response, output);
                else
                {
                    try 
                    {
                        WriteError(m.SupportedErrors, error, output);
                    } 
                    catch (Exception)
                    {    
                        // Presumably no match on the exception, throw the original
                        throw error;
                    }
                }
            }
            catch (Exception e)
            {
                // system error
                log.Warn("system error", e);
                context.Error = e;
                bbo = new ByteBufferOutputStream();
                output = new BinaryEncoder(bbo);
                output.WriteBoolean(true);

                WriteError(errorSchema /*Protocol.SYSTEM_ERRORS*/, e.ToString(), output);
                if (null == handshake)
                {
                    handshake = new ByteBufferOutputStream().GetBufferList();
                }
            }

            output.Flush();
            List<MemoryStream> payload = bbo.GetBufferList();

            // Grab meta-data from plugins
            context.ResponsePayload = payload;

            META_WRITER.Write(context.ResponseCallMeta, output);
            output.Flush();
            // Prepend handshake and append payload
            bbo.Prepend(handshake);
            bbo.Append(payload);

            return bbo.GetBufferList();
        }

        static Schema errorSchema = Schema.Parse("[\"string\"]");
    }
}