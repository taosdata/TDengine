/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.avro.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;

public class ReusableByteBufferInputStream extends InputStream {

  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

  // work around issues compiling with Java11 but running with 8
  // due to ByteBuffer overriding several methods
  private ByteBuffer byteBuffer = EMPTY_BUFFER;
  private Buffer buffer = byteBuffer;
  private int mark = 0;

  public void setByteBuffer(ByteBuffer buf) {
    // do not modify the buffer that is passed in
    this.byteBuffer = buf.duplicate();
    this.buffer = byteBuffer;
    this.mark = buf.position();
  }

  @Override
  public int read() throws IOException {
    if (buffer.hasRemaining()) {
      return byteBuffer.get() & 0xff;
    } else {
      return -1;
    }
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (buffer.remaining() <= 0) {
      return -1;
    }
    // allow IndexOutOfBoundsException to be thrown by ByteBuffer#get
    int bytesToRead = Math.min(len, buffer.remaining());
    byteBuffer.get(b, off, bytesToRead);
    return bytesToRead;
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      // n may be negative and results in skipping 0 bytes, according to javadoc
      return 0;
    }

    // this catches n > Integer.MAX_VALUE
    int bytesToSkip = n > buffer.remaining() ? buffer.remaining() : (int) n;
    buffer.position(buffer.position() + bytesToSkip);
    return bytesToSkip;
  }

  @Override
  public synchronized void mark(int readLimit) {
    // readLimit is ignored. there is no requirement to implement readLimit, it
    // is a way for implementations to avoid buffering too much. since all data
    // for this stream is held in memory, this has no need for such a limit.
    this.mark = buffer.position();
  }

  @Override
  public synchronized void reset() throws IOException {
    buffer.position(mark);
  }

  @Override
  public boolean markSupported() {
    return true;
  }
}
