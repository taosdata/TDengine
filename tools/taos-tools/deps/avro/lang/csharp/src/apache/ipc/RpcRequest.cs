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
using Avro.IO;

namespace Avro.ipc
{
    public class RpcRequest
    {
        private readonly String messageName;
        private readonly Object request;
        private readonly RpcContext context;

        private Message message;
        private List<MemoryStream> requestBytes;

        public RpcRequest(string messageName, object request, RpcContext rpcContext)
        {
            if (messageName == null) throw new ArgumentNullException("messageName");
            if (request == null) throw new ArgumentNullException("request");
            if (rpcContext == null) throw new ArgumentNullException("rpcContext");

            this.messageName = messageName;
            this.request = request;
            context = rpcContext;
        }

        public RpcRequest(RpcRequest request)
            : this(request.messageName, request.request, request.Context)
        {
        }

        public RpcContext Context
        {
            get { return context; }
        }


        public Message GetMessage(Protocol local)
        {
            if (message == null)
            {
                message = local.Messages[messageName];
                if (message == null)
                {
                    throw new AvroRuntimeException("Not a local message: " + messageName);
                }
            }
            return message;
        }

        public IList<MemoryStream> GetBytes(Protocol local, Requestor requestor)
        {
            if (local == null) throw new ArgumentNullException("local");
            if (requestor == null) throw new ArgumentNullException("requestor");

            if (requestBytes == null)
            {
                using (var bbo = new ByteBufferOutputStream())
                {
                    var o = new BinaryEncoder(bbo);

                    // use local protocol to write request
                    Message m = GetMessage(local);
                    Context.Message = m;

                    requestor.WriteRequest(m.Request, request, o); // write request payload

                    o.Flush();
                    List<MemoryStream> payload = bbo.GetBufferList();

                    requestor.WriteHandshake(o); // prepend handshake if needed

                    Context.RequestPayload = payload;

                    IDictionary<string, object> responseCallMeta = Context.ResponseCallMeta;
                    Requestor.MetaWriter.Write(responseCallMeta, o);

                    o.WriteString(m.Name); // write message name
                    o.Flush();

                    bbo.Append(payload);

                    requestBytes = bbo.GetBufferList();
                }
            }

            return requestBytes;
        }
    }
}