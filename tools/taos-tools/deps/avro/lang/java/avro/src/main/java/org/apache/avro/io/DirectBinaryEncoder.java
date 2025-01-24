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
import java.util.Objects;

/**
 * An {@link Encoder} for Avro's binary encoding that does not buffer output.
 * <p/>
 * This encoder does not buffer writes, and as a result is slower than
 * {@link BufferedBinaryEncoder}. However, it is lighter-weight and useful when
 * the buffering in BufferedBinaryEncoder is not desired and/or the Encoder is
 * very short lived.
 * <p/>
 * To construct, use
 * {@link EncoderFactory#directBinaryEncoder(OutputStream, BinaryEncoder)}
 * <p/>
 * DirectBinaryEncoder is not thread-safe
 * 
 * @see BinaryEncoder
 * @see EncoderFactory
 * @see Encoder
 * @see Decoder
 */
public class DirectBinaryEncoder extends BinaryEncoder {
  private OutputStream out;
  // the buffer is used for writing floats, doubles, and large longs.
  private final byte[] buf = new byte[12];

  /**
   * Create a writer that sends its output to the underlying stream
   * <code>out</code>.
   **/
  DirectBinaryEncoder(OutputStream out) {
    configure(out);
  }

  DirectBinaryEncoder configure(OutputStream out) {
    Objects.requireNonNull(out, "OutputStream cannot be null");
    this.out = out;
    return this;
  }

  @Override
  public void flush() throws IOException {
    out.flush();
  }

  @Override
  public void writeBoolean(boolean b) throws IOException {
    out.write(b ? 1 : 0);
  }

  /*
   * buffering is slower for ints that encode to just 1 or two bytes, and and
   * faster for large ones. (Sun JRE 1.6u22, x64 -server)
   */
  @Override
  public void writeInt(int n) throws IOException {
    int val = (n << 1) ^ (n >> 31);
    if ((val & ~0x7F) == 0) {
      out.write(val);
      return;
    } else if ((val & ~0x3FFF) == 0) {
      out.write(0x80 | val);
      out.write(val >>> 7);
      return;
    }
    int len = BinaryData.encodeInt(n, buf, 0);
    out.write(buf, 0, len);
  }

  /*
   * buffering is slower for writeLong when the number is small enough to fit in
   * an int. (Sun JRE 1.6u22, x64 -server)
   */
  @Override
  public void writeLong(long n) throws IOException {
    long val = (n << 1) ^ (n >> 63); // move sign to low-order bit
    if ((val & ~0x7FFFFFFFL) == 0) {
      int i = (int) val;
      while ((i & ~0x7F) != 0) {
        out.write((byte) ((0x80 | i) & 0xFF));
        i >>>= 7;
      }
      out.write((byte) i);
      return;
    }
    int len = BinaryData.encodeLong(n, buf, 0);
    out.write(buf, 0, len);
  }

  @Override
  public void writeFloat(float f) throws IOException {
    int len = BinaryData.encodeFloat(f, buf, 0);
    out.write(buf, 0, len);
  }

  @Override
  public void writeDouble(double d) throws IOException {
    int len = BinaryData.encodeDouble(d, buf, 0);
    out.write(buf, 0, len);
  }

  @Override
  public void writeFixed(byte[] bytes, int start, int len) throws IOException {
    out.write(bytes, start, len);
  }

  @Override
  protected void writeZero() throws IOException {
    out.write(0);
  }

  @Override
  public int bytesBuffered() {
    return 0;
  }

}
