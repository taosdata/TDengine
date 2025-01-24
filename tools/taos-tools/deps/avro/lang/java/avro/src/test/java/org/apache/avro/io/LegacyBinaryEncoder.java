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

import org.apache.avro.util.Utf8;

/**
 * Low-level support for serializing Avro values.
 *
 * This class has two types of methods. One type of methods support the writing
 * of leaf values (for example, {@link #writeLong} and {@link #writeString}).
 * These methods have analogs in {@link Decoder}.
 *
 * The other type of methods support the writing of maps and arrays. These
 * methods are {@link #writeArrayStart}, {@link #startItem}, and
 * {@link #writeArrayEnd} (and similar methods for maps). Some implementations
 * of {@link Encoder} handle the buffering required to break large maps and
 * arrays into blocks, which is necessary for applications that want to do
 * streaming. (See {@link #writeArrayStart} for details on these methods.)
 *
 * @see Decoder
 */
public class LegacyBinaryEncoder extends Encoder {
  protected OutputStream out;

  private interface ByteWriter {
    void write(ByteBuffer bytes) throws IOException;
  }

  private static final class SimpleByteWriter implements ByteWriter {
    private final OutputStream out;

    public SimpleByteWriter(OutputStream out) {
      this.out = out;
    }

    @Override
    public void write(ByteBuffer bytes) throws IOException {
      encodeLong(bytes.remaining(), out);
      out.write(bytes.array(), bytes.position(), bytes.remaining());
    }
  }

  private final ByteWriter byteWriter;

  /**
   * Create a writer that sends its output to the underlying stream
   * <code>out</code>.
   */
  public LegacyBinaryEncoder(OutputStream out) {
    this.out = out;
    this.byteWriter = new SimpleByteWriter(out);
  }

  @Override
  public void flush() throws IOException {
    if (out != null) {
      out.flush();
    }
  }

  @Override
  public void writeNull() throws IOException {
  }

  @Override
  public void writeBoolean(boolean b) throws IOException {
    out.write(b ? 1 : 0);
  }

  @Override
  public void writeInt(int n) throws IOException {
    encodeLong(n, out);
  }

  @Override
  public void writeLong(long n) throws IOException {
    encodeLong(n, out);
  }

  @Override
  public void writeFloat(float f) throws IOException {
    encodeFloat(f, out);
  }

  @Override
  public void writeDouble(double d) throws IOException {
    encodeDouble(d, out);
  }

  @Override
  public void writeString(Utf8 utf8) throws IOException {
    encodeString(utf8.getBytes(), 0, utf8.getByteLength());
  }

  @Override
  public void writeString(String string) throws IOException {
    byte[] bytes = Utf8.getBytesFor(string);
    encodeString(bytes, 0, bytes.length);
  }

  private void encodeString(byte[] bytes, int offset, int length) throws IOException {
    encodeLong(length, out);
    out.write(bytes, offset, length);
  }

  @Override
  public void writeBytes(ByteBuffer bytes) throws IOException {
    byteWriter.write(bytes);
  }

  @Override
  public void writeBytes(byte[] bytes, int start, int len) throws IOException {
    encodeLong(len, out);
    out.write(bytes, start, len);
  }

  @Override
  public void writeFixed(byte[] bytes, int start, int len) throws IOException {
    out.write(bytes, start, len);
  }

  @Override
  public void writeEnum(int e) throws IOException {
    encodeLong(e, out);
  }

  @Override
  public void writeArrayStart() throws IOException {
  }

  @Override
  public void setItemCount(long itemCount) throws IOException {
    if (itemCount > 0) {
      writeLong(itemCount);
    }
  }

  @Override
  public void startItem() throws IOException {
  }

  @Override
  public void writeArrayEnd() throws IOException {
    encodeLong(0, out);
  }

  @Override
  public void writeMapStart() throws IOException {
  }

  @Override
  public void writeMapEnd() throws IOException {
    encodeLong(0, out);
  }

  @Override
  public void writeIndex(int unionIndex) throws IOException {
    encodeLong(unionIndex, out);
  }

  protected static void encodeLong(long n, OutputStream o) throws IOException {
    n = (n << 1) ^ (n >> 63); // move sign to low-order bit
    while ((n & ~0x7F) != 0) {
      o.write((byte) ((n & 0x7f) | 0x80));
      n >>>= 7;
    }
    o.write((byte) n);
  }

  protected static void encodeFloat(float f, OutputStream o) throws IOException {
    long bits = Float.floatToRawIntBits(f);
    o.write((int) (bits) & 0xFF);
    o.write((int) (bits >> 8) & 0xFF);
    o.write((int) (bits >> 16) & 0xFF);
    o.write((int) (bits >> 24) & 0xFF);
  }

  protected static void encodeDouble(double d, OutputStream o) throws IOException {
    long bits = Double.doubleToRawLongBits(d);
    o.write((int) (bits) & 0xFF);
    o.write((int) (bits >> 8) & 0xFF);
    o.write((int) (bits >> 16) & 0xFF);
    o.write((int) (bits >> 24) & 0xFF);
    o.write((int) (bits >> 32) & 0xFF);
    o.write((int) (bits >> 40) & 0xFF);
    o.write((int) (bits >> 48) & 0xFF);
    o.write((int) (bits >> 56) & 0xFF);
  }

}
