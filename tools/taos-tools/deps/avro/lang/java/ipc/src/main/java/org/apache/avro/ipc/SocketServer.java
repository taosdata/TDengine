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

package org.apache.avro.ipc;

import java.io.IOException;
import java.io.EOFException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.Protocol;
import org.apache.avro.Protocol.Message;
import org.apache.avro.ipc.generic.GenericResponder;

/**
 * A socket-based server implementation. This uses a simple, non-standard wire
 * protocol and is not intended for production services.
 *
 * @deprecated use {@link SaslSocketServer} instead.
 */
@Deprecated
public class SocketServer extends Thread implements Server {
  private static final Logger LOG = LoggerFactory.getLogger(SocketServer.class);

  private Responder responder;
  private ServerSocketChannel channel;
  private ThreadGroup group;

  public SocketServer(Responder responder, SocketAddress addr) throws IOException {
    String name = "SocketServer on " + addr;

    this.responder = responder;
    this.group = new ThreadGroup(name);
    this.channel = ServerSocketChannel.open();

    channel.socket().bind(addr);

    setName(name);
    setDaemon(true);
  }

  @Override
  public int getPort() {
    return channel.socket().getLocalPort();
  }

  @Override
  public void run() {
    LOG.info("starting " + channel.socket().getInetAddress());
    try {
      while (true) {
        try {
          new Connection(channel.accept());
        } catch (ClosedChannelException e) {
          return;
        } catch (IOException e) {
          LOG.warn("unexpected error", e);
          throw new RuntimeException(e);
        }
      }
    } finally {
      LOG.info("stopping " + channel.socket().getInetAddress());
      try {
        channel.close();
      } catch (IOException e) {
      }
    }
  }

  @Override
  public void close() {
    this.interrupt();
    group.interrupt();
  }

  /**
   * Creates an appropriate {@link Transceiver} for this server. Returns a
   * {@link SocketTransceiver} by default.
   */
  protected Transceiver getTransceiver(SocketChannel channel) throws IOException {
    return new SocketTransceiver(channel);
  }

  private class Connection implements Runnable {

    SocketChannel channel;
    Transceiver xc;

    public Connection(SocketChannel channel) throws IOException {
      this.channel = channel;

      Thread thread = new Thread(group, this);
      thread.setName("Connection to " + channel.socket().getRemoteSocketAddress());
      thread.setDaemon(true);
      thread.start();
    }

    @Override
    public void run() {
      try {
        try {
          this.xc = getTransceiver(channel);
          while (true) {
            xc.writeBuffers(responder.respond(xc.readBuffers(), xc));
          }
        } catch (EOFException | ClosedChannelException e) {
        } finally {
          xc.close();
        }
      } catch (IOException e) {
        LOG.warn("unexpected error", e);
      }
    }

  }

  public static void main(String[] arg) throws Exception {
    Responder responder = new GenericResponder(Protocol.parse("{\"protocol\": \"X\"}")) {
      @Override
      public Object respond(Message message, Object request) throws Exception {
        throw new AvroRemoteException("no messages!");
      }
    };
    SocketServer server = new SocketServer(responder, new InetSocketAddress(0));
    server.start();
    System.out.println("server started on port: " + server.getPort());
    server.join();
  }
}
