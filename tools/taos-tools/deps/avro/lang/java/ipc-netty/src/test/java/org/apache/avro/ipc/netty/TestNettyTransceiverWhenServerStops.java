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

import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.avro.test.Mail;
import org.apache.avro.test.Message;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.fail;

public class TestNettyTransceiverWhenServerStops {
//  @Test                                           // disable flakey test!
  public void testNettyTransceiverWhenServerStops() throws Exception {
    Mail mailService = new TestNettyServer.MailImpl();
    Responder responder = new SpecificResponder(Mail.class, mailService);
    NettyServer server = new NettyServer(responder, new InetSocketAddress(0));
    server.start();

    int serverPort = server.getPort();

    final NettyTransceiver transceiver = new NettyTransceiver(new InetSocketAddress(serverPort), 60000);
    final Mail mail = SpecificRequestor.getClient(Mail.class, transceiver);

    final AtomicInteger successes = new AtomicInteger();
    final AtomicInteger failures = new AtomicInteger();
    final AtomicBoolean quitOnFailure = new AtomicBoolean();
    List<Thread> threads = new ArrayList<>();

    // Start a bunch of client threads that use the transceiver to send messages
    for (int i = 0; i < 100; i++) {
      Thread thread = new Thread(() -> {
        while (true) {
          try {
            mail.send(createMessage());
            successes.incrementAndGet();
          } catch (Exception e) {
            failures.incrementAndGet();
            if (quitOnFailure.get()) {
              return;
            }
          }
        }
      });
      threads.add(thread);
      thread.start();
    }

    // Be sure the threads are running: wait until we get a good deal of successes
    while (successes.get() < 10000) {
      Thread.sleep(50);
    }

    // Now stop the server
    server.close();

    // Server is stopped: successes should not increase anymore: wait until we're in
    // that situation
    while (true) {
      int previousSuccesses = successes.get();
      Thread.sleep(500);
      if (previousSuccesses == successes.get()) {
        break;
      }
    }

    // Start the server again
    server.start();

    // This part of the test is not solved by the current patch: it shows that when
    // you stop/start
    // a server, the client requests don't continue immediately but will stay
    // blocked until the timeout
    // passed to the NettyTransceiver has passed (IIUC)
    long now = System.currentTimeMillis();
    /*
     * System.out.println("Waiting on requests to continue"); int previousSuccesses
     * = successes.get(); while (true) { Thread.sleep(500); if (successes.get() >
     * previousSuccesses) { break; } if (System.currentTimeMillis() - now > 5000) {
     * System.out.println("FYI: requests don't continue immediately..."); break; } }
     */

    // Stop our client, we would expect this to go on immediately
    System.out.println("Stopping transceiver");
    quitOnFailure.set(true);
    now = System.currentTimeMillis();
    transceiver.close(); // Without the patch, this close seems to hang forever

    // Wait for all threads to quit
    for (Thread thread : threads) {
      thread.join();
    }
    if (System.currentTimeMillis() - now > 10000) {
      fail("Stopping NettyTransceiver and waiting for client threads to quit took too long.");
    } else {
      System.out.println("Stopping NettyTransceiver and waiting for client threads to quit took "
          + (System.currentTimeMillis() - now) + " ms");
    }
  }

  private Message createMessage() {
    Message msg = Message.newBuilder().setTo("wife").setFrom("husband").setBody("I love you!").build();
    return msg;
  }
}
