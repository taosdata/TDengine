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

namespace Avro.ipc
{
    public class LocalTransceiver : Transceiver
    {
        private readonly Responder responder;

        public LocalTransceiver(Responder responder)
        {
            if (responder == null) throw new ArgumentNullException("responder");

            this.responder = responder;
        }

        public override string RemoteName
        {
            get { return "local"; }
        }

        public override IList<MemoryStream> Transceive(IList<MemoryStream> request)
        {
            return responder.Respond(request);
        }

        public override IList<MemoryStream> ReadBuffers()
        {
            throw new NotSupportedException();
        }

        public override void WriteBuffers(IList<MemoryStream> getBytes)
        {
            throw new NotSupportedException();
        }
    }
}