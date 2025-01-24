/*
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

package org.apache.avro.ipc.netty;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.ipc.CallFuture;
import org.apache.avro.ipc.Callback;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.avro.test.Simple;
import org.apache.avro.test.TestError;
import org.apache.avro.test.TestRecord;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests asynchronous RPCs with Netty.
 */
public class TestNettyServerWithCallbacks {
  private static Server server;
  private static Transceiver transceiver;
  private static Simple.Callback simpleClient;
  private static final AtomicBoolean ackFlag = new AtomicBoolean(false);
  private static final AtomicReference<CountDownLatch> ackLatch = new AtomicReference<>(new CountDownLatch(1));
  private static Simple simpleService = new SimpleImpl(ackFlag);

  @BeforeClass
  public static void initializeConnections() throws Exception {
    // start server
    Responder responder = new SpecificResponder(Simple.class, simpleService);
    server = new NettyServer(responder, new InetSocketAddress(0));
    server.start();

    int serverPort = server.getPort();
    System.out.println("server port : " + serverPort);

    transceiver = new NettyTransceiver(new InetSocketAddress(serverPort), TestNettyServer.CONNECT_TIMEOUT_MILLIS);
    simpleClient = SpecificRequestor.getClient(Simple.Callback.class, transceiver);
  }

  @AfterClass
  public static void tearDownConnections() throws Exception {
    if (transceiver != null) {
      transceiver.close();
    }
    if (server != null) {
      server.close();
    }
  }

  @Test
  public void greeting() throws Exception {
    // Test synchronous RPC:
    Assert.assertEquals("Hello, how are you?", simpleClient.hello("how are you?"));

    // Test asynchronous RPC (future):
    CallFuture<String> future1 = new CallFuture<>();
    simpleClient.hello("World!", future1);
    Assert.assertEquals("Hello, World!", future1.get(2, TimeUnit.SECONDS));
    Assert.assertNull(future1.getError());

    // Test asynchronous RPC (callback):
    final CallFuture<String> future2 = new CallFuture<>();
    simpleClient.hello("what's up?", new Callback<String>() {
      @Override
      public void handleResult(String result) {
        future2.handleResult(result);
      }

      @Override
      public void handleError(Throwable error) {
        future2.handleError(error);
      }
    });
    Assert.assertEquals("Hello, what's up?", future2.get(2, TimeUnit.SECONDS));
    Assert.assertNull(future2.getError());
  }

  @Test
  public void echo() throws Exception {
    TestRecord record = TestRecord.newBuilder()
        .setHash(new org.apache.avro.test.MD5(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8 }))
        .setKind(org.apache.avro.test.Kind.FOO).setName("My Record").build();

    // Test synchronous RPC:
    Assert.assertEquals(record, simpleClient.echo(record));

    // Test asynchronous RPC (future):
    CallFuture<TestRecord> future1 = new CallFuture<>();
    simpleClient.echo(record, future1);
    Assert.assertEquals(record, future1.get(2, TimeUnit.SECONDS));
    Assert.assertNull(future1.getError());

    // Test asynchronous RPC (callback):
    final CallFuture<TestRecord> future2 = new CallFuture<>();
    simpleClient.echo(record, new Callback<TestRecord>() {
      @Override
      public void handleResult(TestRecord result) {
        future2.handleResult(result);
      }

      @Override
      public void handleError(Throwable error) {
        future2.handleError(error);
      }
    });
    Assert.assertEquals(record, future2.get(2, TimeUnit.SECONDS));
    Assert.assertNull(future2.getError());
  }

  @Test
  public void add() throws Exception {
    // Test synchronous RPC:
    Assert.assertEquals(8, simpleClient.add(2, 6));

    // Test asynchronous RPC (future):
    CallFuture<Integer> future1 = new CallFuture<>();
    simpleClient.add(8, 8, future1);
    Assert.assertEquals(Integer.valueOf(16), future1.get(2, TimeUnit.SECONDS));
    Assert.assertNull(future1.getError());

    // Test asynchronous RPC (callback):
    final CallFuture<Integer> future2 = new CallFuture<>();
    simpleClient.add(512, 256, new Callback<Integer>() {
      @Override
      public void handleResult(Integer result) {
        future2.handleResult(result);
      }

      @Override
      public void handleError(Throwable error) {
        future2.handleError(error);
      }
    });
    Assert.assertEquals(Integer.valueOf(768), future2.get(2, TimeUnit.SECONDS));
    Assert.assertNull(future2.getError());
  }

  @Test
  public void echoBytes() throws Exception {
    ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 });

    // Test synchronous RPC:
    Assert.assertEquals(byteBuffer, simpleClient.echoBytes(byteBuffer));

    // Test asynchronous RPC (future):
    CallFuture<ByteBuffer> future1 = new CallFuture<>();
    simpleClient.echoBytes(byteBuffer, future1);
    Assert.assertEquals(byteBuffer, future1.get(2, TimeUnit.SECONDS));
    Assert.assertNull(future1.getError());

    // Test asynchronous RPC (callback):
    final CallFuture<ByteBuffer> future2 = new CallFuture<>();
    simpleClient.echoBytes(byteBuffer, new Callback<ByteBuffer>() {
      @Override
      public void handleResult(ByteBuffer result) {
        future2.handleResult(result);
      }

      @Override
      public void handleError(Throwable error) {
        future2.handleError(error);
      }
    });
    Assert.assertEquals(byteBuffer, future2.get(2, TimeUnit.SECONDS));
    Assert.assertNull(future2.getError());
  }

  @Test()
  public void error() throws IOException, InterruptedException, TimeoutException {
    // Test synchronous RPC:
    try {
      simpleClient.error();
      Assert.fail("Expected " + TestError.class.getCanonicalName());
    } catch (TestError e) {
      // Expected
    }

    // Test asynchronous RPC (future):
    CallFuture<Void> future = new CallFuture<>();
    simpleClient.error(future);
    try {
      future.get(2, TimeUnit.SECONDS);
      Assert.fail("Expected " + TestError.class.getCanonicalName() + " to be thrown");
    } catch (ExecutionException e) {
      Assert.assertTrue("Expected " + TestError.class.getCanonicalName(), e.getCause() instanceof TestError);
    }
    Assert.assertNotNull(future.getError());
    Assert.assertTrue("Expected " + TestError.class.getCanonicalName(), future.getError() instanceof TestError);
    Assert.assertNull(future.getResult());

    // Test asynchronous RPC (callback):
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicReference<Throwable> errorRef = new AtomicReference<>();
    simpleClient.error(new Callback<Void>() {
      @Override
      public void handleResult(Void result) {
        Assert.fail("Expected " + TestError.class.getCanonicalName());
      }

      @Override
      public void handleError(Throwable error) {
        errorRef.set(error);
        latch.countDown();
      }
    });
    Assert.assertTrue("Timed out waiting for error", latch.await(2, TimeUnit.SECONDS));
    Assert.assertNotNull(errorRef.get());
    Assert.assertTrue(errorRef.get() instanceof TestError);
  }

  @Test
  public void ack() throws Exception {
    simpleClient.ack();
    ackLatch.get().await(2, TimeUnit.SECONDS);
    Assert.assertTrue("Expected ack flag to be set", ackFlag.get());

    ackLatch.set(new CountDownLatch(1));
    simpleClient.ack();
    ackLatch.get().await(2, TimeUnit.SECONDS);
    Assert.assertFalse("Expected ack flag to be cleared", ackFlag.get());
  }

  @Test
  public void testSendAfterChannelClose() throws Exception {
    // Start up a second server so that closing the server doesn't
    // interfere with the other unit tests:
    Server server2 = new NettyServer(new SpecificResponder(Simple.class, simpleService), new InetSocketAddress(0));
    server2.start();
    try {
      int serverPort = server2.getPort();
      System.out.println("server2 port : " + serverPort);

      try (Transceiver transceiver2 = new NettyTransceiver(new InetSocketAddress(serverPort),
          TestNettyServer.CONNECT_TIMEOUT_MILLIS)) {
        Simple.Callback simpleClient2 = SpecificRequestor.getClient(Simple.Callback.class, transceiver2);

        // Verify that connection works:
        Assert.assertEquals(3, simpleClient2.add(1, 2));

        // Try again with callbacks:
        CallFuture<Integer> addFuture = new CallFuture<>();
        simpleClient2.add(1, 2, addFuture);
        Assert.assertEquals(Integer.valueOf(3), addFuture.get());

        // Shut down server:
        server2.close();
        Thread.sleep(1000L);

        // Send a new RPC, and verify that it throws an Exception that
        // can be detected by the client:
        boolean ioeCaught = false;
        try {
          simpleClient2.add(1, 2);
          Assert.fail("Send after server close should have thrown Exception");
        } catch (AvroRuntimeException e) {
          ioeCaught = e.getCause() instanceof IOException;
          Assert.assertTrue("Expected IOException", ioeCaught);
        } catch (Exception e) {
          e.printStackTrace();
          throw e;
        }
        Assert.assertTrue("Expected IOException", ioeCaught);

        // Send a new RPC with callback, and verify that the correct Exception
        // is thrown:
        ioeCaught = false;
        try {
          addFuture = new CallFuture<>();
          simpleClient2.add(1, 2, addFuture);
          addFuture.get();
          Assert.fail("Send after server close should have thrown Exception");
        } catch (IOException e) {
          ioeCaught = true;
        } catch (Exception e) {
          e.printStackTrace();
          throw e;
        }
        Assert.assertTrue("Expected IOException", ioeCaught);
      }
    } finally {
      server2.close();
    }
  }

  @Test
  public void cancelPendingRequestsOnTransceiverClose() throws Exception {
    // Start up a second server so that closing the server doesn't
    // interfere with the other unit tests:
    BlockingSimpleImpl blockingSimpleImpl = new BlockingSimpleImpl();
    Server server2 = new NettyServer(new SpecificResponder(Simple.class, blockingSimpleImpl), new InetSocketAddress(0));
    server2.start();
    try {
      int serverPort = server2.getPort();
      System.out.println("server2 port : " + serverPort);

      CallFuture<Integer> addFuture = new CallFuture<>();
      try (Transceiver transceiver2 = new NettyTransceiver(new InetSocketAddress(serverPort),
          TestNettyServer.CONNECT_TIMEOUT_MILLIS)) {
        Simple.Callback simpleClient2 = SpecificRequestor.getClient(Simple.Callback.class, transceiver2);

        // The first call has to block for the handshake:
        Assert.assertEquals(3, simpleClient2.add(1, 2));

        // Now acquire the semaphore so that the server will block:
        blockingSimpleImpl.acquireRunPermit();
        simpleClient2.add(1, 2, addFuture);
      }
      // When the transceiver is closed, the CallFuture should get
      // an IOException
      boolean ioeThrown = false;
      try {
        addFuture.get();
      } catch (ExecutionException e) {
        ioeThrown = e.getCause() instanceof IOException;
        Assert.assertTrue(e.getCause() instanceof IOException);
      } catch (Exception e) {
        e.printStackTrace();
        Assert.fail("Unexpected Exception: " + e.toString());
      }
      Assert.assertTrue("Expected IOException to be thrown", ioeThrown);
    } finally {
      blockingSimpleImpl.releaseRunPermit();
      server2.close();
    }
  }

  @Test(timeout = 20000)
  public void cancelPendingRequestsAfterChannelCloseByServerShutdown() throws Throwable {
    // The purpose of this test is to verify that a client doesn't stay
    // blocked when a server is unexpectedly killed (or when for some
    // other reason the channel is suddenly closed) while the server
    // was in the process of handling a request (thus after it received
    // the request, and before it returned the response).

    // Start up a second server so that closing the server doesn't
    // interfere with the other unit tests:
    BlockingSimpleImpl blockingSimpleImpl = new BlockingSimpleImpl();
    final Server server2 = new NettyServer(new SpecificResponder(Simple.class, blockingSimpleImpl),
        new InetSocketAddress(0));
    server2.start();

    Transceiver transceiver2 = null;

    try {
      int serverPort = server2.getPort();
      System.out.println("server2 port : " + serverPort);

      transceiver2 = new NettyTransceiver(new InetSocketAddress(serverPort), TestNettyServer.CONNECT_TIMEOUT_MILLIS);

      final Simple.Callback simpleClient2 = SpecificRequestor.getClient(Simple.Callback.class, transceiver2);

      // Acquire the method-enter permit, which will be released by the
      // server method once we call it
      blockingSimpleImpl.acquireEnterPermit();

      // Acquire the run permit, to avoid that the server method returns immediately
      blockingSimpleImpl.acquireRunPermit();

      // Start client call
      Future<?> clientFuture = Executors.newSingleThreadExecutor().submit(() -> {
        try {
          simpleClient2.add(3, 4);
          Assert.fail("Expected an exception");
        } catch (Exception e) {
          e.printStackTrace();
          // expected
        }
      });

      // Wait until method is entered on the server side
      blockingSimpleImpl.acquireEnterPermit();

      // The server side method is now blocked waiting on the run permit
      // (= is busy handling the request)

      // Stop the server in a separate thread as it blocks the actual thread until the
      // server side
      // method is running
      new Thread(server2::close).start();

      // With the server gone, we expect the client to get some exception and exit
      // Wait for the client call to exit
      try {
        clientFuture.get(10, TimeUnit.SECONDS);
      } catch (ExecutionException e) {
        throw e.getCause();
      } catch (TimeoutException e) {
        Assert.fail("Client request should not be blocked on server shutdown");
      }

    } finally {
      blockingSimpleImpl.releaseRunPermit();
      server2.close();
      if (transceiver2 != null)
        transceiver2.close();
    }
  }

  @Test
  public void clientReconnectAfterServerRestart() throws Exception {
    // Start up a second server so that closing the server doesn't
    // interfere with the other unit tests:
    SimpleImpl simpleImpl = new BlockingSimpleImpl();
    Server server2 = new NettyServer(new SpecificResponder(Simple.class, simpleImpl), new InetSocketAddress(0));
    try {
      server2.start();
      int serverPort = server2.getPort();
      System.out.println("server2 port : " + serverPort);

      // Initialize a client, and establish a connection to the server:
      Transceiver transceiver2 = new NettyTransceiver(new InetSocketAddress(serverPort),
          TestNettyServer.CONNECT_TIMEOUT_MILLIS);
      Simple.Callback simpleClient2 = SpecificRequestor.getClient(Simple.Callback.class, transceiver2);
      Assert.assertEquals(3, simpleClient2.add(1, 2));

      // Restart the server:
      server2.close();
      try {
        simpleClient2.add(2, -1);
        Assert.fail("Client should not be able to invoke RPCs " + "because server is no longer running");
      } catch (Exception e) {
        // Expected since server is no longer running
      }
      Thread.sleep(2000L);
      server2 = new NettyServer(new SpecificResponder(Simple.class, simpleImpl), new InetSocketAddress(serverPort));
      server2.start();

      // Invoke an RPC using the same client, which should reestablish the
      // connection to the server:
      Assert.assertEquals(3, simpleClient2.add(1, 2));
    } finally {
      server2.close();
    }
  }

  @Ignore
  @Test
  public void performanceTest() throws Exception {
    final int threadCount = 8;
    final long runTimeMillis = 10 * 1000L;
    ExecutorService threadPool = Executors.newFixedThreadPool(threadCount);

    System.out.println("Running performance test for " + runTimeMillis + "ms...");
    final AtomicLong rpcCount = new AtomicLong(0L);
    final AtomicBoolean runFlag = new AtomicBoolean(true);
    final CountDownLatch startLatch = new CountDownLatch(threadCount);
    for (int ii = 0; ii < threadCount; ii++) {
      threadPool.submit(() -> {
        try {
          startLatch.countDown();
          startLatch.await(2, TimeUnit.SECONDS);
          while (runFlag.get()) {
            rpcCount.incrementAndGet();
            Assert.assertEquals("Hello, World!", simpleClient.hello("World!"));
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
    }

    startLatch.await(2, TimeUnit.SECONDS);
    Thread.sleep(runTimeMillis);
    runFlag.set(false);
    threadPool.shutdown();
    Assert.assertTrue("Timed out shutting down thread pool", threadPool.awaitTermination(2, TimeUnit.SECONDS));
    System.out.println("Completed " + rpcCount.get() + " RPCs in " + runTimeMillis + "ms => "
        + (((double) rpcCount.get() / (double) runTimeMillis) * 1000) + " RPCs/sec, "
        + ((double) runTimeMillis / (double) rpcCount.get()) + " ms/RPC.");
  }

  /**
   * Implementation of the Simple interface.
   */
  private static class SimpleImpl implements Simple {
    private final AtomicBoolean ackFlag;

    /**
     * Creates a SimpleImpl.
     *
     * @param ackFlag the AtomicBoolean to toggle when ack() is called.
     */
    public SimpleImpl(final AtomicBoolean ackFlag) {
      this.ackFlag = ackFlag;
    }

    @Override
    public String hello(String greeting) {
      return "Hello, " + greeting;
    }

    @Override
    public TestRecord echo(TestRecord record) {
      return record;
    }

    @Override
    public int add(int arg1, int arg2) {
      return arg1 + arg2;
    }

    @Override
    public ByteBuffer echoBytes(ByteBuffer data) {
      return data;
    }

    @Override
    public void error() throws TestError {
      throw TestError.newBuilder().setMessage$("Test Message").build();
    }

    @Override
    synchronized public void ack() {
      ackFlag.set(!ackFlag.get());
      ackLatch.get().countDown();
    }
  }

  /**
   * A SimpleImpl that requires a semaphore permit before executing any method.
   */
  private static class BlockingSimpleImpl extends SimpleImpl {
    /** Semaphore that is released when the method is entered. */
    private final Semaphore enterSemaphore = new Semaphore(1);
    /** Semaphore that must be acquired for the method to run and exit. */
    private final Semaphore runSemaphore = new Semaphore(1);

    /**
     * Creates a BlockingSimpleImpl.
     */
    public BlockingSimpleImpl() {
      super(new AtomicBoolean());
    }

    @Override
    public String hello(String greeting) {
      releaseEnterPermit();
      acquireRunPermit();
      try {
        return super.hello(greeting);
      } finally {
        releaseRunPermit();
      }
    }

    @Override
    public TestRecord echo(TestRecord record) {
      releaseEnterPermit();
      acquireRunPermit();
      try {
        return super.echo(record);
      } finally {
        releaseRunPermit();
      }
    }

    @Override
    public int add(int arg1, int arg2) {
      releaseEnterPermit();
      acquireRunPermit();
      try {
        return super.add(arg1, arg2);
      } finally {
        releaseRunPermit();
      }
    }

    @Override
    public ByteBuffer echoBytes(ByteBuffer data) {
      releaseEnterPermit();
      acquireRunPermit();
      try {
        return super.echoBytes(data);
      } finally {
        releaseRunPermit();
      }
    }

    @Override
    public void error() throws TestError {
      releaseEnterPermit();
      acquireRunPermit();
      try {
        super.error();
      } finally {
        releaseRunPermit();
      }
    }

    @Override
    public void ack() {
      releaseEnterPermit();
      acquireRunPermit();
      try {
        super.ack();
      } finally {
        releaseRunPermit();
      }
    }

    /**
     * Acquires a single permit from the semaphore.
     */
    public void acquireRunPermit() {
      try {
        runSemaphore.acquire();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }

    /**
     * Releases a single permit to the semaphore.
     */
    public void releaseRunPermit() {
      runSemaphore.release();
    }

    /**
     * Acquires a single permit from the semaphore.
     */
    public void acquireEnterPermit() {
      try {
        enterSemaphore.acquire();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }

    /**
     * Releases a single permit to the semaphore.
     */
    public void releaseEnterPermit() {
      enterSemaphore.release();
    }
  }
}
