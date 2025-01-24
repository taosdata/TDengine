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
using org.apache.avro.ipc;

namespace Avro.ipc
{
    public class RpcContext
    {
        private Exception _error;
        private IDictionary<String, object> _requestCallMeta;
        private Object _response;
        private IDictionary<String, object> _responseCallMeta;

        public HandshakeRequest HandshakeRequest { get; set; }
        public HandshakeResponse HandshakeResponse { get; set; }

        public IList<MemoryStream> RequestPayload { get; set; }
        public IList<MemoryStream> ResponsePayload { get; set; }

        public Exception Error
        {
            set
            {
                _response = null;
                _error = value;
            }
            get { return _error; }
        }

        public Object Response
        {
            set
            {
                _response = value;
                _error = null;
            }

            get { return _response; }
        }


        public IDictionary<String, byte[]> RequestHandshakeMeta
        {
            set { HandshakeRequest.meta = value; }

            get
            {
                if (HandshakeRequest.meta == null)
                    HandshakeRequest.meta = new Dictionary<String, byte[]>();

                return HandshakeRequest.meta;
            }
        }


        public IDictionary<String, byte[]> ResponseHandshakeMeta
        {
            set { HandshakeResponse.meta = value; }

            get
            {
                if (HandshakeResponse.meta == null)
                    HandshakeResponse.meta = new Dictionary<String, byte[]>();

                return HandshakeResponse.meta;
            }
        }

        /**
         * This is an access method for the per-call state
         * provided by the client to the server.
         * @return a map representing per-call state from
         * the client to the server
         */

        public IDictionary<String, object> RequestCallMeta
        {
            get
            {
                if (_requestCallMeta == null)
                {
                    _requestCallMeta = new Dictionary<string, object>();
                }
                return _requestCallMeta;
            }
            set { _requestCallMeta = value; }
        }


        /**
         * This is an access method for the per-call state
         * provided by the server back to the client.
         * @return a map representing per-call state from
         * the server to the client
         */

        public IDictionary<String, object> ResponseCallMeta
        {
            get
            {
                if (_responseCallMeta == null)
                {
                    _responseCallMeta = new Dictionary<String, object>();
                }
                return _responseCallMeta;
            }
            set { _responseCallMeta = value; }
        }

        /**
         * Indicates whether an exception was generated
         * at the server
         * @return true is an exception was generated at
         * the server, or false if not
         */

        public bool IsError
        {
            get { return Error != null; }
        }

        public Message Message { get; set; }
    }
}