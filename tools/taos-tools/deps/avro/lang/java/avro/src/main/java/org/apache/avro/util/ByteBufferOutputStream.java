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

package org.apache.avro.util;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility to collect data written to an {@link OutputStream} in
 * {@link ByteBuffer}s.
 */
public class ByteBufferOutputStream extends OutputStream {
  public static final int BUFFER_SIZE = 8192;

  private List<ByteBuffer> buffers;

  public ByteBufferOutputStream() {
    reset();
  }

  /** Returns all data written and resets the stream to be empty. */
  public List<ByteBuffer> getBufferList() {
    List<ByteBuffer> result = buffers;
    reset();
    for (Buffer buffer : result) {
      buffer.flip();
    }
    return result;
  }

  /** Prepend a list of ByteBuffers to this stream. */
  public void prepend(List<ByteBuffer> lists) {
    for (Buffer buffer : lists) {
      buffer.position(buffer.limit());
    }
    buffers.addAll(0, lists);
  }

  /** Append a list of ByteBuffers to this stream. */
  public void append(List<ByteBuffer> lists) {
    for (Buffer buffer : lists) {
      buffer.position(buffer.limit());
    }
    buffers.addAll(lists);
  }

  public void reset() {
    buffers = new ArrayList<>();
    buffers.add(ByteBuffer.allocate(BUFFER_SIZE));
  }

  public void write(ByteBuffer buffer) {
    buffers.add(buffer);
  }

  @Override
  public void write(int b) {
    ByteBuffer buffer = buffers.get(buffers.size() - 1);
    if (buffer.remaining() < 1) {
      buffer = ByteBuffer.allocate(BUFFER_SIZE);
      buffers.add(buffer);
    }
    buffer.put((byte) b);
  }

  @Override
  public void write(byte[] b, int off, int len) {
    ByteBuffer buffer = buffers.get(buffers.size() - 1);
    int remaining = buffer.remaining();
    while (len > remaining) {
      buffer.put(b, off, remaining);
      len -= remaining;
      off += remaining;
      buffer = ByteBuffer.allocate(BUFFER_SIZE);
      buffers.add(buffer);
      remaining = buffer.remaining();
    }
    buffer.put(b, off, len);
  }

  /** Add a buffer to the output without copying, if possible. */
  public void writeBuffer(ByteBuffer buffer) throws IOException {
    if (buffer.remaining() < BUFFER_SIZE) {
      write(buffer.array(), buffer.position(), buffer.remaining());
    } else { // append w/o copying bytes
      ByteBuffer dup = buffer.duplicate();
      dup.position(buffer.limit()); // ready for flip
      buffers.add(dup);
    }
  }
}
