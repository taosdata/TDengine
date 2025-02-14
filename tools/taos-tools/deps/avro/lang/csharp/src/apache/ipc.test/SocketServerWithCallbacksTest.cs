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
using System.Diagnostics;
using System.Net.Sockets;
using System.Threading;
using Avro.ipc;
using Avro.ipc.Specific;
using NUnit.Framework;
using org.apache.avro.test;

namespace Avro.Test.Ipc
{
    [TestFixture]
    public class SocketServerWithCallbacksTest
    {
        private static volatile bool ackFlag;
        private static volatile CountdownLatch ackLatch = new CountdownLatch(1);

        private SocketServer server;
        private SocketTransceiver transceiver;
        private SimpleCallback simpleClient;

        [OneTimeSetUp]
        public void Init()
        {
            var responder = new SpecificResponder<Simple>(new SimpleImpl());
            server = new SocketServer("localhost", 0, responder);
            server.Start();

            transceiver = new SocketTransceiver("localhost", server.Port);
            simpleClient = SpecificRequestor.CreateClient<SimpleCallback>(transceiver);
        }

        [OneTimeTearDown]
        public void TearDown()
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
        public void CancelPendingRequestsOnTransceiverClose()
        {
            // Start up a second server so that closing the server doesn't
            // interfere with the other unit tests:
            var blockingSimpleImpl = new BlockingSimpleImpl();

            var responder = new SpecificResponder<Simple>(blockingSimpleImpl);
            var server2 = new SocketServer("localhost", 0, responder);

            server2.Start();

            try
            {
                int serverPort = server2.Port;

                var transceiver2 = new SocketTransceiver("localhost", serverPort);

                var addFuture = new CallFuture<int>();
                try
                {
                    var simpleClient2 = SpecificRequestor.CreateClient<SimpleCallback>(transceiver2);

                    // The first call has to block for the handshake:
                    Assert.AreEqual(3, simpleClient2.add(1, 2));

                    // Now acquire the semaphore so that the server will block:
                    blockingSimpleImpl.acquireRunPermit();
                    simpleClient2.add(1, 2, addFuture);
                }
                finally
                {
                    // When the transceiver is closed, the CallFuture should get
                    // an IOException
                    transceiver2.Close();
                }
                bool ioeThrown = false;
                try
                {
                    addFuture.WaitForResult(2000);
                }
                catch (Exception)
                {
                }
                //catch (ExecutionException e) {
                //  ioeThrown = e.getCause() instanceof IOException;
                //  Assert.assertTrue(e.getCause() instanceof IOException);
                //} catch (Exception e) {
                //  e.printStackTrace();
                //  Assert.fail("Unexpected Exception: " + e.toString());
                //}
                Assert.IsTrue(ioeThrown, "Expected IOException to be thrown");
            }
            finally
            {
                blockingSimpleImpl.releaseRunPermit();
                server2.Stop();
            }
        }

        // AVRO-625 [Test]
        public void CancelPendingRequestsAfterChannelCloseByServerShutdown()
        {
            // The purpose of this test is to verify that a client doesn't stay
            // blocked when a server is unexpectedly killed (or when for some
            // other reason the channel is suddenly closed) while the server
            // was in the process of handling a request (thus after it received
            // the request, and before it returned the response).

            // Start up a second server so that closing the server doesn't
            // interfere with the other unit tests:
            var blockingSimpleImpl = new BlockingSimpleImpl();

            var responder = new SpecificResponder<Simple>(blockingSimpleImpl);
            var server2 = new SocketServer("localhost", 0, responder);

            server2.Start();
            SocketTransceiver transceiver2 = null;

            try
            {
                transceiver2 = new SocketTransceiver("localhost", server2.Port);

                var simpleClient2 = SpecificRequestor.CreateClient<SimpleCallback>(transceiver2);

                // Acquire the method-enter permit, which will be released by the
                // server method once we call it
                blockingSimpleImpl.acquireEnterPermit();

                // Acquire the run permit, to avoid that the server method returns immediately
                blockingSimpleImpl.acquireRunPermit();

                var t = new Thread(() =>
                                       {
                                           try
                                           {
                                               simpleClient2.add(3, 4);
                                               Assert.Fail("Expected an exception");
                                           }
                                           catch (Exception)
                                           {
                                               // expected
                                           }
                                       });
                // Start client call
                t.Start();

                // Wait until method is entered on the server side
                blockingSimpleImpl.acquireEnterPermit();

                // The server side method is now blocked waiting on the run permit
                // (= is busy handling the request)

                // Stop the server
                server2.Stop();

                // With the server gone, we expect the client to get some exception and exit
                // Wait for client thread to exit
                t.Join(10000);

                Assert.IsFalse(t.IsAlive, "Client request should not be blocked on server shutdown");
            }
            finally
            {
                blockingSimpleImpl.releaseRunPermit();
                server2.Stop();
                if (transceiver2 != null)
                    transceiver2.Close();
            }
        }


        private class CallbackCallFuture<T> : CallFuture<T>
        {
            private readonly Action<Exception> _handleException;
            private readonly Action<T> _handleResult;

            public CallbackCallFuture(Action<T> handleResult = null, Action<Exception> handleException = null)
            {
                _handleResult = handleResult;
                _handleException = handleException;
            }

            public override void HandleResult(T result)
            {
                _handleResult(result);
            }

            public override void HandleException(Exception exception)
            {
                _handleException(exception);
            }
        }

        private class NestedCallFuture<T> : CallFuture<T>
        {
            private readonly CallFuture<T> cf;

            public NestedCallFuture(CallFuture<T> cf)
            {
                this.cf = cf;
            }

            public override void HandleResult(T result)
            {
                cf.HandleResult(result);
            }

            public override void HandleException(Exception exception)
            {
                cf.HandleException(exception);
            }
        }


        private string Hello(string howAreYou)
        {
            var response = new CallFuture<string>();

            simpleClient.hello(howAreYou, response);

            return response.WaitForResult(2000);
        }

        private void Hello(string howAreYou, CallFuture<string> future1)
        {
            simpleClient.hello(howAreYou, future1);
        }

        private class BlockingSimpleImpl : SimpleImpl
        {
            /** Semaphore that is released when the method is entered. */
            private readonly Semaphore enterSemaphore = new Semaphore(1, 1);
            /** Semaphore that must be acquired for the method to run and exit. */
            private readonly Semaphore runSemaphore = new Semaphore(1, 1);


            public override string hello(string greeting)
            {
                releaseEnterPermit();
                acquireRunPermit();
                try
                {
                    return base.hello(greeting);
                }
                finally
                {
                    releaseRunPermit();
                }
            }

            public override TestRecord echo(TestRecord record)
            {
                releaseEnterPermit();
                acquireRunPermit();
                try
                {
                    return base.echo(record);
                }
                finally
                {
                    releaseRunPermit();
                }
            }

            public override int add(int arg1, int arg2)
            {
                releaseEnterPermit();
                acquireRunPermit();
                try
                {
                    return base.add(arg1, arg2);
                }
                finally
                {
                    releaseRunPermit();
                }
            }

            public override byte[] echoBytes(byte[] data)
            {
                releaseEnterPermit();
                acquireRunPermit();
                try
                {
                    return base.echoBytes(data);
                }
                finally
                {
                    releaseRunPermit();
                }
            }

            public override object error()
            {
                releaseEnterPermit();
                acquireRunPermit();
                try
                {
                    return base.error();
                }
                finally
                {
                    releaseRunPermit();
                }
            }

            public override void ack()
            {
                releaseEnterPermit();
                acquireRunPermit();
                try
                {
                    base.ack();
                }
                finally
                {
                    releaseRunPermit();
                }
            }


            /**
     * Acquires a single permit from the semaphore.
     */

            public void acquireRunPermit()
            {
                try
                {
                    runSemaphore.WaitOne();
                    //  } catch (InterruptedException e) {
                    //    Thread.currentThread().interrupt();
                    //    throw new RuntimeException(e);
                    //}
                }
                finally
                {
                }
            }

            /**
     * Releases a single permit to the semaphore.
     */

            public void releaseRunPermit()
            {
                try
                {
                    runSemaphore.Release();
                }
                catch //(SemaphoreFullException)
                {
                }
            }

            private void releaseEnterPermit()
            {
                try
                {
                    enterSemaphore.Release();
                }
                catch //(SemaphoreFullException)
                {
                }
            }

            /**
     * Acquires a single permit from the semaphore.
     */

            public void acquireEnterPermit()
            {
                try
                {
                    enterSemaphore.WaitOne();
                    //    } catch (InterruptedException e) {
                    //      Thread.currentThread().interrupt();
                    //      throw new RuntimeException(e);
                    //}
                }
                finally
                {
                }
            }
        }

        [Test]
        public void Ack()
        {
            simpleClient.ack();

            ackLatch.Wait(2000);
            Assert.IsTrue(ackFlag, "Expected ack flag to be set");

            ackLatch = new CountdownLatch(1);
            simpleClient.ack();
            ackLatch.Wait(2000);
            Assert.IsFalse(ackFlag, "Expected ack flag to be cleared");
        }

        [Test]
        public void Add()
        {
            // Test synchronous RPC:
            Assert.AreEqual(8, simpleClient.add(2, 6));

            // Test asynchronous RPC (future):
            var future1 = new CallFuture<int>();
            simpleClient.add(8, 8, future1);
            Assert.AreEqual(16, future1.WaitForResult(2000));
            Assert.IsNull(future1.Error);

            // Test asynchronous RPC (callback):
            var future2 = new CallFuture<int>();
            simpleClient.add(512, 256, new NestedCallFuture<int>(future2));

            Assert.AreEqual(768, future2.WaitForResult(2000));
            Assert.IsNull(future2.Error);
        }

        [Test]
        public void ClientReconnectAfterServerRestart()
        {
            // Start up a second server so that closing the server doesn't
            // interfere with the other unit tests:
            SimpleImpl simpleImpl = new BlockingSimpleImpl();

            var responder = new SpecificResponder<Simple>(simpleImpl);
            var server2 = new SocketServer("localhost", 0, responder);

            server2.Start();

            try
            {
                int serverPort = server2.Port;

                // Initialize a client, and establish a connection to the server:
                Transceiver transceiver2 = new SocketTransceiver("localhost", serverPort);

                var simpleClient2 =
                    SpecificRequestor.CreateClient<SimpleCallback>(transceiver2);

                Assert.AreEqual(3, simpleClient2.add(1, 2));

                // Restart the server:
                server2.Stop();
                try
                {
                    simpleClient2.add(2, -1);
                    Assert.Fail("Client should not be able to invoke RPCs because server is no longer running");
                }
                catch (Exception)
                {
                    // Expected since server is no longer running
                }

                Thread.Sleep(2000);
                server2 = new SocketServer("localhost", serverPort, new SpecificResponder<Simple>(new SimpleImpl()));

                server2.Start();

                // Invoke an RPC using the same client, which should reestablish the
                // connection to the server:
                Assert.AreEqual(3, simpleClient2.add(1, 2));
            }
            finally
            {
                server2.Stop();
            }
        }

        [Test]
        public void Echo()
        {
            var record = new TestRecord
                             {
                                 hash =
                                     new MD5
                                         {
                                             Value =
                                                 new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8}
                                         },
                                 kind = Kind.FOO,
                                 name = "My Record"
                             };

            // Test synchronous RPC:
            TestRecord testRecord = simpleClient.echo(record);
            Assert.AreEqual(record, testRecord);

            // Test asynchronous RPC (future):
            var future1 = new CallFuture<TestRecord>();
            simpleClient.echo(record, future1);
            Assert.AreEqual(record, future1.WaitForResult(2000));
            Assert.IsNull(future1.Error);

            // Test asynchronous RPC (callback):
            var future2 = new CallFuture<TestRecord>();
            simpleClient.echo(record, new NestedCallFuture<TestRecord>(future2));

            Assert.AreEqual(record, future2.WaitForResult(2000));
            Assert.IsNull(future2.Error);
        }

        [Test]
        public void EchoBytes()
        {
            var byteBuffer = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};

            // Test synchronous RPC:
            Assert.AreEqual(byteBuffer, simpleClient.echoBytes(byteBuffer));

            // Test asynchronous RPC (future):
            var future1 = new CallFuture<byte[]>();
            simpleClient.echoBytes(byteBuffer, future1);
            Assert.AreEqual(byteBuffer, future1.WaitForResult(2000));
            Assert.IsNull(future1.Error);

            // Test asynchronous RPC (callback):
            var future2 = new CallFuture<byte[]>();
            simpleClient.echoBytes(byteBuffer, new NestedCallFuture<byte[]>(future2));

            Assert.AreEqual(byteBuffer, future2.WaitForResult(2000));
            Assert.IsNull(future2.Error);
        }

        [Test, TestCase(false, TestName = "Specific error"), TestCase(true, TestName = "System error")]
        public void Error(bool systemError)
        {
            Type expected;

            if(systemError)
            {
                expected = typeof(Exception);
                SimpleImpl.throwSystemError = true;
            }
            else
            {
                expected = typeof(TestError);
                SimpleImpl.throwSystemError = false;
            }

            // Test synchronous RPC:
            try
            {
                simpleClient.error();
                Assert.Fail("Expected " + expected.Name + " to be thrown");
            }
            catch (Exception e)
            {
                Assert.AreEqual(expected, e.GetType());
            }

            // Test asynchronous RPC (future):
            var future = new CallFuture<object>();
            simpleClient.error(future);
            try
            {
                future.WaitForResult(2000);
                Assert.Fail("Expected " + expected.Name + " to be thrown");
            }
            catch (Exception e)
            {
                Assert.AreEqual(expected, e.GetType());
            }

            Assert.IsNotNull(future.Error);
            Assert.AreEqual(expected, future.Error.GetType());
            Assert.IsNull(future.Result);

            // Test asynchronous RPC (callback):
            Exception errorRef = null;
            var latch = new CountdownLatch(1);
            simpleClient.error(new CallbackCallFuture<object>(
                                   result => Assert.Fail("Expected " + expected.Name),
                                   exception =>
                                       {
                                           errorRef = exception;
                                           latch.Signal();
                                       }));

            Assert.IsTrue(latch.Wait(2000), "Timed out waiting for error");
            Assert.IsNotNull(errorRef);
            Assert.AreEqual(expected, errorRef.GetType());
        }

        [Test]
        public void Greeting()
        {
            // Test synchronous RPC:
            string response = Hello("how are you?");
            Assert.AreEqual("Hello, how are you?", response);

            // Test asynchronous RPC (future):
            var future1 = new CallFuture<String>();
            Hello("World!", future1);

            string result = future1.WaitForResult();
            Assert.AreEqual("Hello, World!", result);
            Assert.IsNull(future1.Error);

            // Test asynchronous RPC (callback):
            var future2 = new CallFuture<String>();

            Hello("what's up?", new NestedCallFuture<string>(future2));
            Assert.AreEqual("Hello, what's up?", future2.WaitForResult());
            Assert.IsNull(future2.Error);
        }

        //[Test]
        public void PerformanceTest()
        {
            const int threadCount = 8;
            const long runTimeMillis = 10*1000L;


            long rpcCount = 0;
            int[] runFlag = {1};

            var startLatch = new CountdownLatch(threadCount);
            for (int ii = 0; ii < threadCount; ii++)
            {
                new Thread(() =>
                               {
                                   {
                                       try
                                       {
                                           startLatch.Signal();
                                           startLatch.Wait(2000);

                                           while (Interlocked.Add(ref runFlag[0], 0) == 1)
                                           {
                                               Interlocked.Increment(ref rpcCount);
                                               Assert.AreEqual("Hello, World!", simpleClient.hello("World!"));
                                           }
                                       }
                                       catch (Exception e)
                                       {
                                           Console.WriteLine(e);
                                       }
                                   }
                               }).Start();
            }

            startLatch.Wait(2000);
            Thread.Sleep(2000);
            Interlocked.Exchange(ref runFlag[0], 1);

            string results = "Completed " + rpcCount + " RPCs in " + runTimeMillis +
                             "ms => " + ((rpcCount/(double) runTimeMillis)*1000) + " RPCs/sec, " +
                             (runTimeMillis/(double) rpcCount) + " ms/RPC.";

            Debug.WriteLine(results);
        }

        [Test]
        public void TestSendAfterChannelClose()
        {
            // Start up a second server so that closing the server doesn't
            // interfere with the other unit tests:

            var responder = new SpecificResponder<Simple>(new SimpleImpl());
            var server2 = new SocketServer("localhost", 0, responder);

            server2.Start();

            try
            {
                var transceiver2 = new SocketTransceiver("localhost", server2.Port);

                try
                {
                    var simpleClient2 = SpecificRequestor.CreateClient<SimpleCallback>(transceiver2);

                    // Verify that connection works:
                    Assert.AreEqual(3, simpleClient2.add(1, 2));

                    // Try again with callbacks:
                    var addFuture = new CallFuture<int>();
                    simpleClient2.add(1, 2, addFuture);
                    Assert.AreEqual(3, addFuture.WaitForResult(2000));

                    // Shut down server:
                    server2.Stop();

                    // Send a new RPC, and verify that it throws an Exception that
                    // can be detected by the client:
                    bool ioeCaught = false;
                    try
                    {
                        simpleClient2.add(1, 2);
                        Assert.Fail("Send after server close should have thrown Exception");
                    }
                    catch (SocketException)
                    {
                        ioeCaught = true;
                    }

                    Assert.IsTrue(ioeCaught, "Expected IOException");

                    // Send a new RPC with callback, and verify that the correct Exception
                    // is thrown:
                    ioeCaught = false;
                    try
                    {
                        addFuture = new CallFuture<int>();
                        simpleClient2.add(1, 2, addFuture);
                        addFuture.WaitForResult(2000);

                        Assert.Fail("Send after server close should have thrown Exception");
                    }
                    catch (SocketException)
                    {
                        ioeCaught = true;
                    }

                    Assert.IsTrue(ioeCaught, "Expected IOException");
                }
                finally
                {
                    transceiver2.Disconnect();
                }
            }
            finally
            {
                server2.Stop();
                Thread.Sleep(1000);
            }
        }

        private class SimpleImpl : Simple
        {
            public static bool throwSystemError = false;

            public override string hello(string greeting)
            {
                return "Hello, " + greeting;
            }

            public override TestRecord echo(TestRecord record)
            {
                return record;
            }

            public override int add(int arg1, int arg2)
            {
                return arg1 + arg2;
            }

            public override byte[] echoBytes(byte[] data)
            {
                return data;
            }

            public override object error()
            {
                if(throwSystemError)
                    throw new SystemException("System error");
                else
                    throw new TestError { message = "Test Message" };
            }

            public override void ack()
            {
                ackFlag = !ackFlag;
                ackLatch.Signal();
            }
        }

    }
}
