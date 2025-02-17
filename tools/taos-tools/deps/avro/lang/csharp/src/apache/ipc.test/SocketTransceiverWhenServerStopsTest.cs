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
using System.Threading;
using Avro.ipc;
using Avro.ipc.Specific;
using NUnit.Framework;
using org.apache.avro.test;

namespace Avro.Test.Ipc
{
    [TestFixture]
    public class SocketTransceiverWhenServerStopsTest
    {
        private static org.apache.avro.test.Message CreateMessage()
        {
            var msg = new org.apache.avro.test.Message
                          {
                              to = "wife",
                              from = "husband",
                              body = "I love you!"
                          };
            return msg;
        }

        private static readonly DateTime Jan1st1970 = new DateTime
            (1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public static long CurrentTimeMillis()
        {
            return (long) (DateTime.UtcNow - Jan1st1970).TotalMilliseconds;
        }

        public class MailImpl : Mail
        {
            private CountdownLatch allMessages = new CountdownLatch(5);

            // in this simple example just return details of the message
            public override String send(org.apache.avro.test.Message message)
            {
                return "Sent message to [" + message.to +
                       "] from [" + message.from + "] with body [" +
                       message.body + "]";
            }

            public override void fireandforget(org.apache.avro.test.Message message)
            {
                allMessages.Signal();
            }

            public void reset()
            {
                allMessages = new CountdownLatch(5);
            }
        }

        [Test]
        public void TestSocketTransceiverWhenServerStops()
        {
            Responder responder = new SpecificResponder<Mail>(new MailImpl());
            var server = new SocketServer("localhost", 0, responder);

            server.Start();

            var transceiver = new SocketTransceiver("localhost", server.Port);
            var mail = SpecificRequestor.CreateClient<Mail>(transceiver);

            int[] successes = {0};
            int failures = 0;
            int[] quitOnFailure = {0};
            var threads = new List<Thread>();

            // Start a bunch of client threads that use the transceiver to send messages
            for (int i = 0; i < 100; i++)
            {
                var thread = new Thread(
                    () =>
                        {
                            while (true)
                            {
                                try
                                {
                                    mail.send(CreateMessage());
                                    Interlocked.Increment(ref successes[0]);
                                }
                                catch (Exception)
                                {
                                    Interlocked.Increment(ref failures);

                                    if (Interlocked.Add(ref quitOnFailure[0], 0) == 1)
                                    {
                                        return;
                                    }
                                }
                            }
                        });

                thread.Name = "Thread" + i;
                threads.Add(thread);
                thread.Start();
            }

            // Be sure the threads are running: wait until we get a good deal of successes
            while (Interlocked.Add(ref successes[0], 0) < 10000)
            {
                Thread.Sleep(50);
            }

            // Now stop the server
            server.Stop();

            // Server is stopped: successes should not increase anymore: wait until we're in that situation
            while (true)
            {
                int previousSuccesses = Interlocked.Add(ref successes[0], 0);
                Thread.Sleep(500);
                if (previousSuccesses == Interlocked.Add(ref successes[0], 0))
                {
                    break;
                }
            }

            server.Start();

            long now = CurrentTimeMillis();

            int previousSuccesses2 = successes[0];
            while (true)
            {
                Thread.Sleep(500);
                if (successes[0] > previousSuccesses2)
                {
                    break;
                }
                if (CurrentTimeMillis() - now > 5000)
                {
                    Console.WriteLine("FYI: requests don't continue immediately...");
                    break;
                }
            }

            // Stop our client, we would expect this to go on immediately
            Console.WriteLine("Stopping transceiver");

            Interlocked.Add(ref quitOnFailure[0], 1);
            now = CurrentTimeMillis();
            transceiver.Close();

            // Wait for all threads to quit
            while (true)
            {
                threads.RemoveAll(x => !x.IsAlive);

                if (threads.Count > 0)
                    Thread.Sleep(1000);
                else
                    break;
            }

            if (CurrentTimeMillis() - now > 10000)
            {
                Assert.Fail("Stopping NettyTransceiver and waiting for client threads to quit took too long.");
            }
            else
            {
                Console.WriteLine("Stopping NettyTransceiver and waiting for client threads to quit took "
                                  + (CurrentTimeMillis() - now) + " ms");
            }
        }
    }
}