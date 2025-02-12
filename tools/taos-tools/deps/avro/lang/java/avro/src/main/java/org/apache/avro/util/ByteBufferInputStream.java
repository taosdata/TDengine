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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;

/** Utility to present {@link ByteBuffer} data as an {@link InputStream}. */
public class ByteBufferInputStream extends InputStream {
  private List<ByteBuffer> buffers;
  private int current;

  public ByteBufferInputStream(List<ByteBuffer> buffers) {
    this.buffers = buffers;
  }

  /**
   * @see InputStream#read()
   */
  @Override
  public int read() throws IOException {
    ByteBuffer buffer = getBuffer();
    if (buffer == null) {
      return -1;
    }
    return buffer.get() & 0xff;
  }

  /**
   * @see InputStream#read(byte[], int, int)
   */
  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (len == 0)
      return 0;
    ByteBuffer buffer = getBuffer();
    if (buffer == null) {
      return -1;
    }
    int remaining = buffer.remaining();
    if (len > remaining) {
      buffer.get(b, off, remaining);
      return remaining;
    } else {
      buffer.get(b, off, len);
      return len;
    }
  }

  /**
   * Read a buffer from the input without copying, if possible.
   */
  public ByteBuffer readBuffer(int length) throws IOException {
    if (length == 0)
      return ByteBuffer.allocate(0);
    ByteBuffer buffer = getBuffer();
    if (buffer == null) {
      return ByteBuffer.allocate(0);
    }
    if (buffer.remaining() == length) { // can return current as-is?
      current++;
      return buffer; // return w/o copying
    }
    // punt: allocate a new buffer & copy into it
    ByteBuffer result = ByteBuffer.allocate(length);
    int start = 0;
    while (start < length)
      start += read(result.array(), start, length - start);
    return result;
  }

  /**
   * Returns the next non-empty buffer.
   */
  private ByteBuffer getBuffer() throws IOException {
    while (current < buffers.size()) {
      ByteBuffer buffer = buffers.get(current);
      if (buffer.hasRemaining())
        return buffer;
      current++;
    }
    return null;
  }
}
