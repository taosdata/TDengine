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
import java.io.OutputStream;
import static org.junit.Assert.assertEquals;

import io.netty.channel.socket.SocketChannel;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.Assert;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.avro.test.Mail;
import org.apache.avro.test.Message;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestNettyServer {
  static final int CONNECT_TIMEOUT_MILLIS = 2000; // 2 sec
  protected static Server server;
  protected static Transceiver transceiver;
  protected static Mail proxy;
  protected static MailImpl mailService;
  protected static Consumer<SocketChannel> channelInitializer;

  public static class MailImpl implements Mail {

    private CountDownLatch allMessages = new CountDownLatch(5);

    // in this simple example just return details of the message
    public String send(Message message) {
      return "Sent message to [" + message.getTo() + "] from [" + message.getFrom() + "] with body ["
          + message.getBody() + "]";
    }

    public void fireandforget(Message message) {
      allMessages.countDown();
    }

    private void awaitMessages() throws InterruptedException {
      allMessages.await(2, TimeUnit.SECONDS);
    }

    private void assertAllMessagesReceived() {
      assertEquals(0, allMessages.getCount());
    }

    public void reset() {
      allMessages = new CountDownLatch(5);
    }
  }

  public static void initializeConnections(Consumer<SocketChannel> initializer) throws Exception {
    initializeConnections(initializer, initializer);
  }

  public static void initializeConnections(Consumer<SocketChannel> serverInitializer,
      Consumer<SocketChannel> transceiverInitializer) throws Exception {
    System.out.println("starting server...");
    channelInitializer = transceiverInitializer;
    mailService = new MailImpl();
    Responder responder = new SpecificResponder(Mail.class, mailService);
    server = new NettyServer(responder, new InetSocketAddress(0), serverInitializer);
    server.start();

    int serverPort = server.getPort();
    System.out.println("server port : " + serverPort);

    transceiver = new NettyTransceiver(new InetSocketAddress(serverPort), CONNECT_TIMEOUT_MILLIS,
        transceiverInitializer, null);
    proxy = SpecificRequestor.getClient(Mail.class, transceiver);
  }

  @BeforeClass
  public static void initializeConnections() throws Exception {
    initializeConnections(null);
  }

  @AfterClass
  public static void tearDownConnections() throws Exception {
    transceiver.close();
    server.close();
  }

  @Test
  public void testRequestResponse() throws Exception {
    for (int x = 0; x < 5; x++) {
      verifyResponse(proxy.send(createMessage()));
    }
  }

  private void verifyResponse(String result) {
    Assert.assertEquals("Sent message to [wife] from [husband] with body [I love you!]", result);
  }

  @Test
  public void testOneway() throws Exception {
    for (int x = 0; x < 5; x++) {
      proxy.fireandforget(createMessage());
    }
    mailService.awaitMessages();
    mailService.assertAllMessagesReceived();
  }

  @Test
  public void testMixtureOfRequests() throws Exception {
    mailService.reset();
    for (int x = 0; x < 5; x++) {
      Message createMessage = createMessage();
      proxy.fireandforget(createMessage);
      verifyResponse(proxy.send(createMessage));
    }
    mailService.awaitMessages();
    mailService.assertAllMessagesReceived();

  }

  @Test
  public void testConnectionsCount() throws Exception {
    Transceiver transceiver2 = new NettyTransceiver(new InetSocketAddress(server.getPort()), CONNECT_TIMEOUT_MILLIS,
        channelInitializer);
    Mail proxy2 = SpecificRequestor.getClient(Mail.class, transceiver2);
    proxy.fireandforget(createMessage());
    proxy2.fireandforget(createMessage());
    Assert.assertEquals(2, ((NettyServer) server).getNumActiveConnections());
    transceiver2.close();

    // Check the active connections with some retries as closing at the client
    // side might not take effect on the server side immediately
    int numActiveConnections = ((NettyServer) server).getNumActiveConnections();
    for (int i = 0; i < 50 && numActiveConnections == 2; ++i) {
      System.out.println("Server still has 2 active connections; retrying...");
      Thread.sleep(100);
      numActiveConnections = ((NettyServer) server).getNumActiveConnections();
    }
    Assert.assertEquals(1, numActiveConnections);
  }

  private Message createMessage() {
    Message msg = Message.newBuilder().setTo("wife").setFrom("husband").setBody("I love you!").build();
    return msg;
  }

  // send a malformed request (HTTP) to the NettyServer port
  @Test
  public void testBadRequest() throws IOException {
    int port = server.getPort();
    String msg = "GET /status HTTP/1.1\n\n";
    InetSocketAddress sockAddr = new InetSocketAddress("127.0.0.1", port);

    try (Socket sock = new Socket()) {
      sock.connect(sockAddr);
      OutputStream out = sock.getOutputStream();
      out.write(msg.getBytes(StandardCharsets.UTF_8));
      out.flush();
      byte[] buf = new byte[2048];
      int bytesRead = sock.getInputStream().read(buf);
      Assert.assertTrue("Connection should have been closed: " + bytesRead, bytesRead == -1);
    }
  }

}
