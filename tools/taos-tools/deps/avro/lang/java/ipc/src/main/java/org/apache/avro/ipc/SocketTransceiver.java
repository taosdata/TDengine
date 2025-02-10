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
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.avro.Protocol;

/**
 * A socket-based {@link Transceiver} implementation. This uses a simple,
 * non-standard wire protocol and is not intended for production services.
 * 
 * @deprecated use {@link SaslSocketTransceiver} instead.
 */
public class SocketTransceiver extends Transceiver {
  private static final Logger LOG = LoggerFactory.getLogger(SocketTransceiver.class);

  private SocketChannel channel;
  private ByteBuffer header = ByteBuffer.allocate(4);

  private Protocol remote;

  public SocketTransceiver(SocketAddress address) throws IOException {
    this(SocketChannel.open(address));
  }

  public SocketTransceiver(SocketChannel channel) throws IOException {
    this.channel = channel;
    this.channel.socket().setTcpNoDelay(true);
    LOG.info("open to " + getRemoteName());
  }

  @Override
  public String getRemoteName() {
    return channel.socket().getRemoteSocketAddress().toString();
  }

  @Override
  public synchronized List<ByteBuffer> readBuffers() throws IOException {
    List<ByteBuffer> buffers = new ArrayList<>();
    while (true) {
      ((Buffer) header).clear();
      while (header.hasRemaining()) {
        if (channel.read(header) < 0)
          throw new ClosedChannelException();
      }
      ((Buffer) header).flip();
      int length = header.getInt();
      if (length == 0) { // end of buffers
        return buffers;
      }
      ByteBuffer buffer = ByteBuffer.allocate(length);
      while (buffer.hasRemaining()) {
        if (channel.read(buffer) < 0)
          throw new ClosedChannelException();
      }
      ((Buffer) buffer).flip();
      buffers.add(buffer);
    }
  }

  @Override
  public synchronized void writeBuffers(List<ByteBuffer> buffers) throws IOException {
    if (buffers == null)
      return; // no data to write
    for (ByteBuffer buffer : buffers) {
      if (buffer.limit() == 0)
        continue;
      writeLength(buffer.limit()); // length-prefix
      channel.write(buffer);
    }
    writeLength(0); // null-terminate
  }

  private void writeLength(int length) throws IOException {
    ((Buffer) header).clear();
    header.putInt(length);
    ((Buffer) header).flip();
    channel.write(header);
  }

  @Override
  public boolean isConnected() {
    return remote != null;
  }

  @Override
  public void setRemote(Protocol remote) {
    this.remote = remote;
  }

  @Override
  public Protocol getRemote() {
    return remote;
  }

  @Override
  public void close() throws IOException {
    if (channel.isOpen()) {
      LOG.info("closing to " + getRemoteName());
      channel.close();
    }
  }

}
