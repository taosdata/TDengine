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
package org.apache.trevni;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Used to write values. */
class OutputBuffer extends ByteArrayOutputStream {
  static final int BLOCK_SIZE = 64 * 1024;

  private int bitCount; // position in booleans

  public OutputBuffer() {
    super((BLOCK_SIZE + BLOCK_SIZE) >> 2);
  }

  public boolean isFull() {
    return size() >= BLOCK_SIZE;
  }

  public ByteBuffer asByteBuffer() {
    return ByteBuffer.wrap(buf, 0, count);
  }

  public void writeValue(Object value, ValueType type) throws IOException {
    switch (type) {
    case NULL:
      break;
    case BOOLEAN:
      writeBoolean((Boolean) value);
      break;
    case INT:
      writeInt((Integer) value);
      break;
    case LONG:
      writeLong((Long) value);
      break;
    case FIXED32:
      writeFixed32((Integer) value);
      break;
    case FIXED64:
      writeFixed64((Long) value);
      break;
    case FLOAT:
      writeFloat((Float) value);
      break;
    case DOUBLE:
      writeDouble((Double) value);
      break;
    case STRING:
      writeString((String) value);
      break;
    case BYTES:
      if (value instanceof ByteBuffer)
        writeBytes((ByteBuffer) value);
      else
        writeBytes((byte[]) value);
      break;
    default:
      throw new TrevniRuntimeException("Unknown value type: " + type);
    }
  }

  public void writeBoolean(boolean value) {
    if (bitCount == 0) { // first bool in byte
      ensure(1);
      count++;
    }
    if (value)
      buf[count - 1] |= (byte) (1 << bitCount);
    bitCount++;
    if (bitCount == 8)
      bitCount = 0;
  }

  public void writeLength(int length) throws IOException {
    bitCount = 0;
    writeInt(length);
  }

  public void writeString(String string) throws IOException {
    byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
    writeInt(bytes.length);
    write(bytes, 0, bytes.length);
  }

  public void writeBytes(ByteBuffer bytes) {
    int pos = bytes.position();
    int start = bytes.arrayOffset() + pos;
    int len = bytes.limit() - pos;
    writeBytes(bytes.array(), start, len);
  }

  public void writeBytes(byte[] bytes) {
    writeBytes(bytes, 0, bytes.length);
  }

  public void writeBytes(byte[] bytes, int start, int len) {
    writeInt(len);
    write(bytes, start, len);
  }

  public void writeFloat(float f) throws IOException {
    writeFixed32(Float.floatToRawIntBits(f));
  }

  public void writeDouble(double d) throws IOException {
    writeFixed64(Double.doubleToRawLongBits(d));
  }

  public void writeFixed32(int i) throws IOException {
    ensure(4);
    buf[count] = (byte) ((i) & 0xFF);
    buf[count + 1] = (byte) ((i >>> 8) & 0xFF);
    buf[count + 2] = (byte) ((i >>> 16) & 0xFF);
    buf[count + 3] = (byte) ((i >>> 24) & 0xFF);
    count += 4;
  }

  public void writeFixed64(long l) throws IOException {
    ensure(8);
    int first = (int) (l & 0xFFFFFFFF);
    int second = (int) ((l >>> 32) & 0xFFFFFFFF);
    buf[count] = (byte) ((first) & 0xFF);
    buf[count + 4] = (byte) ((second) & 0xFF);
    buf[count + 5] = (byte) ((second >>> 8) & 0xFF);
    buf[count + 1] = (byte) ((first >>> 8) & 0xFF);
    buf[count + 2] = (byte) ((first >>> 16) & 0xFF);
    buf[count + 6] = (byte) ((second >>> 16) & 0xFF);
    buf[count + 7] = (byte) ((second >>> 24) & 0xFF);
    buf[count + 3] = (byte) ((first >>> 24) & 0xFF);
    count += 8;
  }

  public void writeInt(int n) {
    ensure(5);
    n = (n << 1) ^ (n >> 31); // move sign to low-order bit
    if ((n & ~0x7F) != 0) {
      buf[count++] = (byte) ((n | 0x80) & 0xFF);
      n >>>= 7;
      if (n > 0x7F) {
        buf[count++] = (byte) ((n | 0x80) & 0xFF);
        n >>>= 7;
        if (n > 0x7F) {
          buf[count++] = (byte) ((n | 0x80) & 0xFF);
          n >>>= 7;
          if (n > 0x7F) {
            buf[count++] = (byte) ((n | 0x80) & 0xFF);
            n >>>= 7;
          }
        }
      }
    }
    buf[count++] = (byte) n;
  }

  public void writeLong(long n) throws IOException {
    ensure(10);
    n = (n << 1) ^ (n >> 63); // move sign to low-order bit
    if ((n & ~0x7FL) != 0) {
      buf[count++] = (byte) ((n | 0x80) & 0xFF);
      n >>>= 7;
      if (n > 0x7F) {
        buf[count++] = (byte) ((n | 0x80) & 0xFF);
        n >>>= 7;
        if (n > 0x7F) {
          buf[count++] = (byte) ((n | 0x80) & 0xFF);
          n >>>= 7;
          if (n > 0x7F) {
            buf[count++] = (byte) ((n | 0x80) & 0xFF);
            n >>>= 7;
            if (n > 0x7F) {
              buf[count++] = (byte) ((n | 0x80) & 0xFF);
              n >>>= 7;
              if (n > 0x7F) {
                buf[count++] = (byte) ((n | 0x80) & 0xFF);
                n >>>= 7;
                if (n > 0x7F) {
                  buf[count++] = (byte) ((n | 0x80) & 0xFF);
                  n >>>= 7;
                  if (n > 0x7F) {
                    buf[count++] = (byte) ((n | 0x80) & 0xFF);
                    n >>>= 7;
                    if (n > 0x7F) {
                      buf[count++] = (byte) ((n | 0x80) & 0xFF);
                      n >>>= 7;
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    buf[count++] = (byte) n;
  }

  private void ensure(int n) {
    if (count + n > buf.length)
      buf = Arrays.copyOf(buf, Math.max(buf.length << 1, count + n));
  }

  public static int size(Object value, ValueType type) {
    switch (type) {
    case NULL:
      return 0;
    case INT:
      return size((Integer) value);
    case LONG:
      return size((Long) value);
    case FIXED32:
    case FLOAT:
      return 4;
    case FIXED64:
    case DOUBLE:
      return 8;
    case STRING:
      return size((String) value);
    case BYTES:
      if (value instanceof ByteBuffer)
        return size((ByteBuffer) value);
      return size((byte[]) value);
    default:
      throw new TrevniRuntimeException("Unknown value type: " + type);
    }
  }

  public static int size(int n) {
    n = (n << 1) ^ (n >> 31); // move sign to low-order bit
    if (n <= (1 << (7 * 1)) - 1)
      return 1;
    if (n <= (1 << (7 * 2)) - 1)
      return 2;
    if (n <= (1 << (7 * 3)) - 1)
      return 3;
    if (n <= (1 << (7 * 4)) - 1)
      return 4;
    return 5;
  }

  public static int size(long n) {
    n = (n << 1) ^ (n >> 63); // move sign to low-order bit
    if (n <= (1 << (7 * 1)) - 1)
      return 1;
    if (n <= (1 << (7 * 2)) - 1)
      return 2;
    if (n <= (1 << (7 * 3)) - 1)
      return 3;
    if (n <= (1 << (7 * 4)) - 1)
      return 4;
    if (n <= (1 << (7 * 5)) - 1)
      return 5;
    if (n <= (1 << (7 * 6)) - 1)
      return 6;
    if (n <= (1 << (7 * 7)) - 1)
      return 7;
    if (n <= (1 << (7 * 8)) - 1)
      return 8;
    if (n <= (1 << (7 * 9)) - 1)
      return 9;
    return 10;
  }

  public static int size(ByteBuffer bytes) {
    int length = bytes.remaining();
    return size(length) + length;
  }

  public static int size(byte[] bytes) {
    int length = bytes.length;
    return size(length) + length;
  }

  public static int size(String string) {
    int length = utf8Length(string);
    return size(length) + length;
  }

  private static int utf8Length(String string) {
    int stringLength = string.length();
    int utf8Length = 0;
    for (int i = 0; i < stringLength; i++) {
      char c = string.charAt(i);
      int p = c; // code point
      if (Character.isHighSurrogate(c) // surrogate pair
          && i != stringLength - 1 && Character.isLowSurrogate(string.charAt(i + 1))) {
        p = string.codePointAt(i);
        i++;
      }
      if (p <= 0x007F) {
        utf8Length += 1;
      } else if (p <= 0x07FF) {
        utf8Length += 2;
      } else if (p <= 0x0FFFF) {
        utf8Length += 3;
      } else if (p <= 0x01FFFFF) {
        utf8Length += 4;
      } else if (p <= 0x03FFFFFF) {
        utf8Length += 5;
      } else {
        utf8Length += 6;
      }
    }
    return utf8Length;
  }

}
