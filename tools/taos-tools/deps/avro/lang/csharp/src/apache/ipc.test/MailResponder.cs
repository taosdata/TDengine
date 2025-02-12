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
using System.IO;
using System.Reflection;
using Avro.Generic;
using Avro.IO;
using Avro.ipc;
using Avro.ipc.Generic;
using NUnit.Framework;

namespace Avro.Test.Ipc
{
    public class MailResponder : GenericResponder
    {
        private static Protocol protocol;
        private CountdownLatch allMessages = new CountdownLatch(5);

        public MailResponder()
            : base(Protocol)
        {
        }

        public static Protocol Protocol
        {
            get
            {
                if (protocol == null)
                {
                    string readAllLines;
                    using (
                        Stream stream =
                            Assembly.GetExecutingAssembly().GetManifestResourceStream("Avro.ipc.test.mail.avpr"))
                    using (var reader = new StreamReader(stream))
                    {
                        readAllLines = reader.ReadToEnd();
                    }

                    protocol = Protocol.Parse(readAllLines);
                }

                return protocol;
            }
        }

        public override object Respond(Message message, object request)
        {
            if (message.Name == "send")
            {
                var genericRecord = (GenericRecord) ((GenericRecord) request)["message"];

                return "Sent message to [" + genericRecord["to"] +
                       "] from [" + genericRecord["from"] + "] with body [" +
                       genericRecord["body"] + "]";
            }
            if (message.Name == "fireandforget")
            {
                allMessages.Signal();
                return null;
            }

            throw new NotSupportedException();
        }

        public void Reset()
        {
            allMessages = new CountdownLatch(5);
        }

        public void AwaitMessages()
        {
            allMessages.Wait(2000);
        }

        public void AssertAllMessagesReceived()
        {
            Assert.AreEqual(0, allMessages.CurrentCount);
        }


        public override void WriteError(Schema schema, object error, Encoder output)
        {
            Assert.Fail(error.ToString());
        }
    }
}
