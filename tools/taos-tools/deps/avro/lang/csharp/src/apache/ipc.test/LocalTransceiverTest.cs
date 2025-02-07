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
using Avro.Generic;
using Avro.IO;
using Avro.ipc;
using Avro.ipc.Generic;
using NUnit.Framework;

namespace Avro.Test.Ipc
{
    [TestFixture]
    public class LocalTransceiverTest
    {
        [TestCase]
        public void TestSingleRpc()
        {
            Transceiver t = new LocalTransceiver(new TestResponder(protocol));
            var p = new GenericRecord(protocol.Messages["m"].Request);
            p.Add("x", "hello");
            var r = new GenericRequestor(t, protocol);

            for (int x = 0; x < 5; x++)
            {
                object request = r.Request("m", p);
                Assert.AreEqual("there", request);
            }
        }


        private readonly Protocol protocol = Protocol.Parse("" + "{\"protocol\": \"Minimal\", "
                                                            + "\"messages\": { \"m\": {"
                                                            +
                                                            "   \"request\": [{\"name\": \"x\", \"type\": \"string\"}], "
                                                            + "   \"response\": \"string\"} } }");

        public class TestResponder : GenericResponder
        {
            public TestResponder(Protocol local)
                : base(local)
            {
            }

            public override object Respond(Message message, object request)
            {
                Assert.AreEqual("hello", ((GenericRecord) request)["x"]);
                return "there";
            }

            public override void WriteError(Schema schema, object error, Encoder output)
            {
                throw new NotSupportedException();
            }
        }
    }
}