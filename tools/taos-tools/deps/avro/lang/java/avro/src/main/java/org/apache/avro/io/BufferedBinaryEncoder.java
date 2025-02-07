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
package org.apache.avro.io;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Objects;

import org.apache.avro.AvroRuntimeException;

/**
 * An {@link Encoder} for Avro's binary encoding.
 * <p/>
 * This implementation buffers output to enhance performance. Output may not
 * appear on the underlying output until flush() is called.
 * <p/>
 * {@link DirectBinaryEncoder} can be used in place of this implementation if
 * the buffering semantics are not desired, and the performance difference is
 * acceptable.
 * <p/>
 * To construct or reconfigure, use
 * {@link EncoderFactory#binaryEncoder(OutputStream, BinaryEncoder)}.
 * <p/>
 * To change the buffer size, configure the factory instance used to create
 * instances with {@link EncoderFactory#configureBufferSize(int)}
 * 
 * @see Encoder
 * @see EncoderFactory
 * @see BlockingBinaryEncoder
 * @see DirectBinaryEncoder
 */
public class BufferedBinaryEncoder extends BinaryEncoder {
  private byte[] buf;
  private int pos;
  private ByteSink sink;
  private int bulkLimit;

  BufferedBinaryEncoder(OutputStream out, int bufferSize) {
    configure(out, bufferSize);
  }

  BufferedBinaryEncoder configure(OutputStream out, int bufferSize) {
    Objects.requireNonNull(out, "OutputStream cannot be null");
    if (null != this.sink) {
      if (pos > 0) {
        try {
          flushBuffer();
        } catch (IOException e) {
          throw new AvroRuntimeException("Failure flushing old output", e);
        }
      }
    }
    this.sink = new OutputStreamSink(out);
    pos = 0;
    if (null == buf || buf.length != bufferSize) {
      buf = new byte[bufferSize];
    }
    bulkLimit = buf.length >>> 1;
    if (bulkLimit > 512) {
      bulkLimit = 512;
    }
    return this;
  }

  @Override
  public void flush() throws IOException {
    flushBuffer();
    sink.innerFlush();
  }

  /**
   * Flushes the internal buffer to the underlying output. Does not flush the
   * underlying output.
   */
  private void flushBuffer() throws IOException {
    if (pos > 0) {
      try {
        sink.innerWrite(buf, 0, pos);
      } finally {
        pos = 0;
      }
    }
  }

  /**
   * Ensures that the buffer has at least num bytes free to write to between its
   * current position and the end. This will not expand the buffer larger than its
   * current size, for writes larger than or near to the size of the buffer, we
   * flush the buffer and write directly to the output, bypassing the buffer.
   * 
   * @param num
   * @throws IOException
   */
  private void ensureBounds(int num) throws IOException {
    int remaining = buf.length - pos;
    if (remaining < num) {
      flushBuffer();
    }
  }

  @Override
  public void writeBoolean(boolean b) throws IOException {
    // inlined, shorter version of ensureBounds
    if (buf.length == pos) {
      flushBuffer();
    }
    pos += BinaryData.encodeBoolean(b, buf, pos);
  }

  @Override
  public void writeInt(int n) throws IOException {
    ensureBounds(5);
    pos += BinaryData.encodeInt(n, buf, pos);
  }

  @Override
  public void writeLong(long n) throws IOException {
    ensureBounds(10);
    pos += BinaryData.encodeLong(n, buf, pos);
  }

  @Override
  public void writeFloat(float f) throws IOException {
    ensureBounds(4);
    pos += BinaryData.encodeFloat(f, buf, pos);
  }

  @Override
  public void writeDouble(double d) throws IOException {
    ensureBounds(8);
    pos += BinaryData.encodeDouble(d, buf, pos);
  }

  @Override
  public void writeFixed(byte[] bytes, int start, int len) throws IOException {
    if (len > bulkLimit) {
      // too big, write direct
      flushBuffer();
      sink.innerWrite(bytes, start, len);
      return;
    }
    ensureBounds(len);
    System.arraycopy(bytes, start, buf, pos, len);
    pos += len;
  }

  @Override
  public void writeFixed(ByteBuffer bytes) throws IOException {
    ByteBuffer readOnlyBytes = bytes.asReadOnlyBuffer();
    if (!bytes.hasArray() && bytes.remaining() > bulkLimit) {
      flushBuffer();
      sink.innerWrite(readOnlyBytes); // bypass the readOnlyBytes
    } else {
      super.writeFixed(readOnlyBytes);
    }
  }

  @Override
  protected void writeZero() throws IOException {
    writeByte(0);
  }

  private void writeByte(int b) throws IOException {
    if (pos == buf.length) {
      flushBuffer();
    }
    buf[pos++] = (byte) (b & 0xFF);
  }

  @Override
  public int bytesBuffered() {
    return pos;
  }

  /**
   * ByteSink abstracts the destination of written data from the core workings of
   * BinaryEncoder.
   * <p/>
   * Currently the only destination option is an OutputStream, but we may later
   * want to handle other constructs or specialize for certain OutputStream
   * Implementations such as ByteBufferOutputStream.
   * <p/>
   */
  private abstract static class ByteSink {
    protected ByteSink() {
    }

    /** Write data from bytes, starting at off, for len bytes **/
    protected abstract void innerWrite(byte[] bytes, int off, int len) throws IOException;

    protected abstract void innerWrite(ByteBuffer buff) throws IOException;

    /** Flush the underlying output, if supported **/
    protected abstract void innerFlush() throws IOException;
  }

  static class OutputStreamSink extends ByteSink {
    private final OutputStream out;
    private final WritableByteChannel channel;

    private OutputStreamSink(OutputStream out) {
      super();
      this.out = out;
      channel = Channels.newChannel(out);
    }

    @Override
    protected void innerWrite(byte[] bytes, int off, int len) throws IOException {
      out.write(bytes, off, len);
    }

    @Override
    protected void innerFlush() throws IOException {
      out.flush();
    }

    @Override
    protected void innerWrite(ByteBuffer buff) throws IOException {
      channel.write(buff);
    }
  }
}
