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

import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.avro.test.Simple;
import org.apache.avro.test.TestError;
import org.apache.avro.test.TestRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Verifies that RPCs executed by different client threads using the same
 * NettyTransceiver will execute concurrently. The test follows these steps: 1.
 * Execute the {@link #org.apache.avro.test.Simple.add(int, int)} RPC to
 * complete the Avro IPC handshake. 2a. In a background thread, wait for the
 * waitLatch. 3a. In the main thread, invoke
 * {@link #org.apache.avro.test.Simple.hello(String)} with the argument "wait".
 * This causes the ClientImpl running on the server to count down the wait
 * latch, which will unblock the background thread and allow it to proceed.
 * After counting down the latch, this call blocks, waiting for
 * {@link #org.apache.avro.test.Simple.ack()} to be invoked. 2b. The background
 * thread wakes up because the waitLatch has been counted down. Now we know that
 * some thread is executing inside hello(String). Next, execute
 * {@link #org.apache.avro.test.Simple.ack()} in the background thread, which
 * will allow the thread executing hello(String) to return. 3b. The thread
 * executing hello(String) on the server unblocks (since ack() has been called),
 * allowing hello(String) to return. 4. If control returns to the main thread,
 * we know that two RPCs (hello(String) and ack()) were executing concurrently.
 */
public class TestNettyServerConcurrentExecution {
  private Server server;
  private Transceiver transceiver;

  @After
  public void cleanUpAfter() throws Exception {
    try {
      if (transceiver != null) {
        transceiver.close();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    try {
      if (server != null) {
        server.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test(timeout = 30000)
  public void test() throws Exception {
    final CountDownLatch waitLatch = new CountDownLatch(1);
    server = new NettyServer(new SpecificResponder(Simple.class, new SimpleImpl(waitLatch)), new InetSocketAddress(0));
    server.start();

    transceiver = new NettyTransceiver(new InetSocketAddress(server.getPort()), TestNettyServer.CONNECT_TIMEOUT_MILLIS);

    // 1. Create the RPC proxy, and establish the handshake:
    final Simple.Callback simpleClient = SpecificRequestor.getClient(Simple.Callback.class, transceiver);
    SpecificRequestor.getRemote(simpleClient); // force handshake

    /*
     * 2a. In a background thread, wait for the Client.hello("wait") call to be
     * received by the server, then: 2b. Execute the Client.ack() RPC, which will
     * unblock the Client.hello("wait") call, allowing it to return to the main
     * thread.
     */
    new Thread() {
      @Override
      public void run() {
        setName(TestNettyServerConcurrentExecution.class.getSimpleName() + "Ack Thread");
        try {
          // Step 2a:
          waitLatch.await();

          // Step 2b:
          simpleClient.ack();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }.start();

    /*
     * 3. Execute the Client.hello("wait") RPC, which will block until the
     * Client.ack() call has completed in the background thread.
     */
    String response = simpleClient.hello("wait");

    // 4. If control reaches here, both RPCs have executed concurrently
    Assert.assertEquals("wait", response);
    Thread.sleep(2000);
  }

  /**
   * Implementation of the Simple interface for use with this unit test. If
   * {@link #hello(String)} is called with "wait" as its argument,
   * {@link #waitLatch} will be counted down, and {@link #hello(String)} will
   * block until {@link #ack()} has been invoked.
   */
  private static class SimpleImpl implements Simple {
    private final CountDownLatch waitLatch;
    private final CountDownLatch ackLatch = new CountDownLatch(1);

    /**
     * Creates a SimpleImpl that uses the given CountDownLatch.
     *
     * @param waitLatch the CountDownLatch to use in {@link #hello(String)}.
     */
    public SimpleImpl(final CountDownLatch waitLatch) {
      this.waitLatch = waitLatch;
    }

    @Override
    public int add(int arg1, int arg2) {
      // Step 1:
      System.out.println("Adding " + arg1 + "+" + arg2);
      return arg1 + arg2;
    }

    @Override
    public String hello(String greeting) {
      if (greeting.equals("wait")) {
        try {
          // Step 3a:
          waitLatch.countDown();

          // Step 3b:
          ackLatch.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return e.toString();
        }
      }
      return greeting;
    }

    @Override
    public void ack() {
      // Step 2b:
      ackLatch.countDown();
    }

    // All RPCs below this line are irrelevant to this test:

    @Override
    public TestRecord echo(TestRecord record) {
      return record;
    }

    @Override
    public ByteBuffer echoBytes(ByteBuffer data) {
      return data;
    }

    @Override
    public void error() throws TestError {
      throw new TestError("TestError");
    }
  }
}
