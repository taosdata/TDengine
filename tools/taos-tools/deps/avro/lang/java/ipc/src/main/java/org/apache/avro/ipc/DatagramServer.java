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
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A datagram-based server implementation. This uses a simple, non-standard wire
 * protocol and is not intended for production services.
 */
public class DatagramServer extends Thread implements Server {
  private static final Logger LOG = LoggerFactory.getLogger(DatagramServer.class);

  private final Responder responder;
  private final DatagramChannel channel;
  private final Transceiver transceiver;

  public DatagramServer(Responder responder, SocketAddress addr) throws IOException {
    String name = "DatagramServer on " + addr;

    this.responder = responder;

    this.channel = DatagramChannel.open();
    channel.socket().bind(addr);

    this.transceiver = new DatagramTransceiver(channel);

    setName(name);
    setDaemon(true);
  }

  @Override
  public int getPort() {
    return channel.socket().getLocalPort();
  }

  @Override
  public void run() {
    while (true) {
      try {
        transceiver.writeBuffers(responder.respond(transceiver.readBuffers()));
      } catch (ClosedChannelException e) {
        return;
      } catch (IOException e) {
        LOG.warn("unexpected error", e);
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void close() {
    this.interrupt();
  }

  public static void main(String[] arg) throws Exception {
    DatagramServer server = new DatagramServer(null, new InetSocketAddress(0));
    server.start();
    System.out.println("started");
    server.join();
  }

}
