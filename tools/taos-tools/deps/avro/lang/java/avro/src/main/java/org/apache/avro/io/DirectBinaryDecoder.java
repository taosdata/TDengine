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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import org.apache.avro.InvalidNumberEncodingException;
import org.apache.avro.util.ByteBufferInputStream;

/**
 * A non-buffering version of {@link BinaryDecoder}.
 * <p/>
 * This implementation will not read-ahead from the provided InputStream beyond
 * the minimum required to service the API requests.
 *
 * @see Encoder
 */

class DirectBinaryDecoder extends BinaryDecoder {
  private InputStream in;

  private class ByteReader {
    public ByteBuffer read(ByteBuffer old, int length) throws IOException {
      ByteBuffer result;
      if (old != null && length <= old.capacity()) {
        result = old;
        ((Buffer) result).clear();
      } else {
        result = ByteBuffer.allocate(length);
      }
      doReadBytes(result.array(), result.position(), length);
      ((Buffer) result).limit(length);
      return result;
    }
  }

  private class ReuseByteReader extends ByteReader {
    private final ByteBufferInputStream bbi;

    public ReuseByteReader(ByteBufferInputStream bbi) {
      this.bbi = bbi;
    }

    @Override
    public ByteBuffer read(ByteBuffer old, int length) throws IOException {
      if (old != null) {
        return super.read(old, length);
      } else {
        return bbi.readBuffer(length);
      }
    }

  }

  private ByteReader byteReader;

  DirectBinaryDecoder(InputStream in) {
    super();
    configure(in);
  }

  DirectBinaryDecoder configure(InputStream in) {
    this.in = in;
    byteReader = (in instanceof ByteBufferInputStream) ? new ReuseByteReader((ByteBufferInputStream) in)
        : new ByteReader();
    return this;
  }

  @Override
  public boolean readBoolean() throws IOException {
    int n = in.read();
    if (n < 0) {
      throw new EOFException();
    }
    return n == 1;
  }

  @Override
  public int readInt() throws IOException {
    int n = 0;
    int b;
    int shift = 0;
    do {
      b = in.read();
      if (b >= 0) {
        n |= (b & 0x7F) << shift;
        if ((b & 0x80) == 0) {
          return (n >>> 1) ^ -(n & 1); // back to two's-complement
        }
      } else {
        throw new EOFException();
      }
      shift += 7;
    } while (shift < 32);
    throw new InvalidNumberEncodingException("Invalid int encoding");

  }

  @Override
  public long readLong() throws IOException {
    long n = 0;
    int b;
    int shift = 0;
    do {
      b = in.read();
      if (b >= 0) {
        n |= (b & 0x7FL) << shift;
        if ((b & 0x80) == 0) {
          return (n >>> 1) ^ -(n & 1); // back to two's-complement
        }
      } else {
        throw new EOFException();
      }
      shift += 7;
    } while (shift < 64);
    throw new InvalidNumberEncodingException("Invalid long encoding");
  }

  private final byte[] buf = new byte[8];

  @Override
  public float readFloat() throws IOException {
    doReadBytes(buf, 0, 4);
    int n = (((int) buf[0]) & 0xff) | ((((int) buf[1]) & 0xff) << 8) | ((((int) buf[2]) & 0xff) << 16)
        | ((((int) buf[3]) & 0xff) << 24);
    return Float.intBitsToFloat(n);
  }

  @Override
  public double readDouble() throws IOException {
    doReadBytes(buf, 0, 8);
    long n = (((long) buf[0]) & 0xff) | ((((long) buf[1]) & 0xff) << 8) | ((((long) buf[2]) & 0xff) << 16)
        | ((((long) buf[3]) & 0xff) << 24) | ((((long) buf[4]) & 0xff) << 32) | ((((long) buf[5]) & 0xff) << 40)
        | ((((long) buf[6]) & 0xff) << 48) | ((((long) buf[7]) & 0xff) << 56);
    return Double.longBitsToDouble(n);
  }

  @Override
  public ByteBuffer readBytes(ByteBuffer old) throws IOException {
    int length = readInt();
    return byteReader.read(old, length);
  }

  @Override
  protected void doSkipBytes(long length) throws IOException {
    while (length > 0) {
      long n = in.skip(length);
      if (n <= 0) {
        throw new EOFException();
      }
      length -= n;
    }
  }

  @Override
  protected void doReadBytes(byte[] bytes, int start, int length) throws IOException {
    for (;;) {
      int n = in.read(bytes, start, length);
      if (n == length || length == 0) {
        return;
      } else if (n < 0) {
        throw new EOFException();
      }
      start += n;
      length -= n;
    }
  }

  @Override
  public InputStream inputStream() {
    return in;
  }

  @Override
  public boolean isEnd() throws IOException {
    throw new UnsupportedOperationException();
  }
}
