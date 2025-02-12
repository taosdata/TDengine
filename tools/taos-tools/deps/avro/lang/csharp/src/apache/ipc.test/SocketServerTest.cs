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
using System.Net.Sockets;
using Avro.Generic;
using Avro.ipc;
using Avro.ipc.Generic;
using NUnit.Framework;

namespace Avro.Test.Ipc
{
    [TestFixture]
    public class SocketServerTest
    {
        private SocketServer server;
        private MailResponder mailResponder;
        private SocketTransceiver transceiver;
        private GenericRequestor proxy;

        [OneTimeSetUp]
        public void Init()
        {
            mailResponder = new MailResponder();

            server = new SocketServer("localhost", 0, mailResponder);
            server.Start();

            transceiver = new SocketTransceiver("localhost", server.Port);
            proxy = new GenericRequestor(transceiver, MailResponder.Protocol);
        }

        [OneTimeTearDown]
        public void Cleanup()
        {
            server.Stop();

            transceiver.Disconnect();
        }

        public void Reset()
        {
            Cleanup();
            Init();
        }

        private string Send(GenericRecord message)
        {
            var request = new GenericRecord(MailResponder.Protocol.Messages["send"].Request);
            request.Add("message", message);

            var result = (string) proxy.Request("send", request);
            return result;
        }

        private static void FireAndForget(GenericRequestor proxy, GenericRecord genericRecord)
        {
            var request = new GenericRecord(MailResponder.Protocol.Messages["fireandforget"].Request);
            request.Add("message", genericRecord);

            proxy.Request("fireandforget", request);
        }

        private void FireAndForget(GenericRecord genericRecord)
        {
            FireAndForget(proxy, genericRecord);
        }

        private static byte[] GetBytes(string str)
        {
            var bytes = new byte[str.Length*sizeof (char)];
            Buffer.BlockCopy(str.ToCharArray(), 0, bytes, 0, bytes.Length);
            return bytes;
        }

        public static GenericRecord CreateMessage()
        {
            // The first and only type in the list is the Message type.
            var recordSchema = (RecordSchema) MailResponder.Protocol.Types[0];
            var record = new GenericRecord(recordSchema);

            record.Add("to", "wife");
            record.Add("from", "husband");
            record.Add("body", "I love you!");

            return record;
        }

        public static void VerifyResponse(string result)
        {
            Assert.AreEqual(
                "Sent message to [wife] from [husband] with body [I love you!]",
                result);
        }

        [Test]
        public void TestBadRequest()
        {
            int port = server.Port;
            const string msg = "GET /status HTTP/1.1\n\n";
            var clientSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            clientSocket.Connect("localhost", port);

            clientSocket.Send(GetBytes(msg));
            var buf = new byte[2048];

            try
            {
                clientSocket.Receive(buf);
            }
            catch (SocketException ex)
            {
                Assert.AreEqual(ex.ErrorCode, (int)SocketError.ConnectionReset);
            }
        }

        [Test]
        public void TestMixtureOfRequests()
        {
            mailResponder.Reset();
            for (int x = 0; x < 5; x++)
            {
                var createMessage = CreateMessage();
                FireAndForget(createMessage);

                var result = Send(createMessage);
                VerifyResponse(result);
            }
            mailResponder.AwaitMessages();
            mailResponder.AssertAllMessagesReceived();
        }

        [Test]
        public void TestMultipleConnectionsCount()
        {
            Reset();

            var transceiver2 = new SocketTransceiver("localhost", server.Port);

            var proxy2 = new GenericRequestor(transceiver2, MailResponder.Protocol);

            FireAndForget(proxy, CreateMessage());
            FireAndForget(proxy2, CreateMessage());
            transceiver2.Disconnect();
        }

        [Test]
        public void TestOneway()
        {
            Reset();
            for (int x = 0; x < 5; x++)
            {
                GenericRecord genericRecord = CreateMessage();
                FireAndForget(genericRecord);
            }
            mailResponder.AwaitMessages();
            mailResponder.AssertAllMessagesReceived();
        }

        [Test]
        public void TestRequestResponse()
        {
            for (int x = 0; x < 5; x++)
            {
                var message = CreateMessage();

                var result = Send(message);
                VerifyResponse(result);
            }
        }
    }
}
