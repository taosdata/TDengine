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
using System.Threading;
using Avro.Generic;
using Avro.IO;
using Avro.ipc;
using Avro.ipc.Generic;
using NUnit.Framework;

namespace Avro.Test.Ipc
{
    [TestFixture]
    public class SocketServerConcurrentExecutionTest
    {
        private SocketServer server;

        private SocketTransceiver transceiver;
        private GenericRequestor proxy;

        //[TearDown]
        public void Cleanup()
        {
            try
            {
                if (transceiver != null)
                {
                    transceiver.Disconnect();
                }
            }
            catch
            {
            }

            try
            {
                server.Stop();
            }
            catch
            {
            }
        }

        // AVRO-625 [Test]
        // Currently, SocketTransceiver does not permit out-of-order requests on a stateful connection.
        public void Test()
        {
            var waitLatch = new CountdownLatch(1);
            var simpleResponder = new SimpleResponder(waitLatch);
            server = new SocketServer("localhost", 0, simpleResponder);

            server.Start();

            int port = server.Port;

            transceiver = new SocketTransceiver("localhost", port);
            proxy = new GenericRequestor(transceiver, SimpleResponder.Protocol);

            // Step 1:
            proxy.GetRemote(); // force handshake

            new Thread(x =>
                           {
                               // Step 2a:
                               waitLatch.Wait();

                               var ack = new GenericRecord(SimpleResponder.Protocol.Messages["ack"].Request);
                               // Step 2b:
                               proxy.Request("ack", ack);

                           }).Start();


            /*
             * 3. Execute the Client.hello("wait") RPC, which will block until the
             *    Client.ack() call has completed in the background thread.
             */

            var request = new GenericRecord(SimpleResponder.Protocol.Messages["hello"].Request);
            request.Add("greeting", "wait");

            var response = (string)proxy.Request("hello", request);

            // 4. If control reaches here, both RPCs have executed concurrently
            Assert.AreEqual("wait", response);
        }

        private class SimpleResponder : GenericResponder
        {
            private readonly CountdownLatch waitLatch;
            private readonly CountdownLatch ackLatch = new CountdownLatch(1);

            static readonly public Protocol Protocol = Protocol.Parse("{\"protocol\":\"Simple\",\"namespace\":\"org.apache.avro.test\",\"doc\":\"Protocol used for testing.\",\"version\":\"1.6.2\",\"javaAnnotation\":[\"javax.annotation.Generated(\\\"avro\\\")\",\"org.apache.avro.TestAnnotation\"],\"types\":[{\"type\":\"enum\",\"name\":\"Kind\",\"symbols\":[\"FOO\",\"BAR\",\"BAZ\"],\"javaAnnotation\":\"org.apache.avro.TestAnnotation\"},{\"type\":\"fixed\",\"name\":\"MD5\",\"size\":16,\"javaAnnotation\":\"org.apache.avro.TestAnnotation\"},{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[{\"name\":\"name\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"order\":\"ignore\",\"javaAnnotation\":\"org.apache.avro.TestAnnotation\"},{\"name\":\"kind\",\"type\":\"Kind\",\"order\":\"descending\"},{\"name\":\"hash\",\"type\":\"MD5\"}],\"javaAnnotation\":\"org.apache.avro.TestAnnotation\"},{\"type\":\"error\",\"name\":\"TestError\",\"fields\":[{\"name\":\"message\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}]},{\"type\":\"record\",\"name\":\"TestRecordWithUnion\",\"fields\":[{\"name\":\"kind\",\"type\":[\"null\",\"Kind\"]},{\"name\":\"value\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}]}]}],\"messages\":{\"hello\":{\"doc\":\"Send a greeting\",\"request\":[{\"name\":\"greeting\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}],\"response\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},\"echo\":{\"doc\":\"Pretend you're in a cave!\",\"request\":[{\"name\":\"record\",\"type\":\"TestRecord\"}],\"response\":\"TestRecord\"},\"add\":{\"specialProp\":\"test\",\"request\":[{\"name\":\"arg1\",\"type\":\"int\"},{\"name\":\"arg2\",\"type\":\"int\"}],\"response\":\"int\"},\"echoBytes\":{\"request\":[{\"name\":\"data\",\"type\":\"bytes\"}],\"response\":\"bytes\"},\"error\":{\"doc\":\"Always throws an error.\",\"request\":[],\"response\":\"null\",\"errors\":[\"TestError\"]},\"ack\":{\"doc\":\"Send a one way message\",\"javaAnnotation\":\"org.apache.avro.TestAnnotation\",\"request\":[],\"response\":\"null\",\"one-way\":true}}}");

            public SimpleResponder(CountdownLatch waitLatch)
                : base(Protocol)
            {
                this.waitLatch = waitLatch;
            }

            public override object Respond(Message message, object request)
            {
                if (message.Name == "hello")
                {
                    string greeting = ((GenericRecord)request)["greeting"].ToString();
                    if (greeting == "wait")
                    {
                        // Step 3a:
                        waitLatch.Signal();

                        // Step 3b:
                        ackLatch.Wait();
                    }
                    return greeting;
                }
                if (message.Name == "ack")
                {
                    ackLatch.Signal();
                }

                throw new NotSupportedException();
            }

            public override void WriteError(Schema schema, object error, Encoder output)
            {
                throw new System.NotImplementedException();
            }
        }
    }
}