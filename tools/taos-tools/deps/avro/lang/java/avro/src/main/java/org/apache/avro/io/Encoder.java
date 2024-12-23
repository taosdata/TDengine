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

import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.util.Utf8;

/**
 * Low-level support for serializing Avro values.
 * <p/>
 * This class has two types of methods. One type of methods support the writing
 * of leaf values (for example, {@link #writeLong} and {@link #writeString}).
 * These methods have analogs in {@link Decoder}.
 * <p/>
 * The other type of methods support the writing of maps and arrays. These
 * methods are {@link #writeArrayStart}, {@link #startItem}, and
 * {@link #writeArrayEnd} (and similar methods for maps). Some implementations
 * of {@link Encoder} handle the buffering required to break large maps and
 * arrays into blocks, which is necessary for applications that want to do
 * streaming. (See {@link #writeArrayStart} for details on these methods.)
 * <p/>
 * {@link EncoderFactory} contains Encoder construction and configuration
 * facilities.
 * 
 * @see EncoderFactory
 * @see Decoder
 */
public abstract class Encoder implements Flushable {

  /**
   * "Writes" a null value. (Doesn't actually write anything, but advances the
   * state of the parser if this class is stateful.)
   * 
   * @throws AvroTypeException If this is a stateful writer and a null is not
   *                           expected
   */
  public abstract void writeNull() throws IOException;

  /**
   * Write a boolean value.
   * 
   * @throws AvroTypeException If this is a stateful writer and a boolean is not
   *                           expected
   */
  public abstract void writeBoolean(boolean b) throws IOException;

  /**
   * Writes a 32-bit integer.
   * 
   * @throws AvroTypeException If this is a stateful writer and an integer is not
   *                           expected
   */
  public abstract void writeInt(int n) throws IOException;

  /**
   * Write a 64-bit integer.
   * 
   * @throws AvroTypeException If this is a stateful writer and a long is not
   *                           expected
   */
  public abstract void writeLong(long n) throws IOException;

  /**
   * Write a float.
   * 
   * @throws IOException
   * @throws AvroTypeException If this is a stateful writer and a float is not
   *                           expected
   */
  public abstract void writeFloat(float f) throws IOException;

  /**
   * Write a double.
   * 
   * @throws AvroTypeException If this is a stateful writer and a double is not
   *                           expected
   */
  public abstract void writeDouble(double d) throws IOException;

  /**
   * Write a Unicode character string.
   * 
   * @throws AvroTypeException If this is a stateful writer and a char-string is
   *                           not expected
   */
  public abstract void writeString(Utf8 utf8) throws IOException;

  /**
   * Write a Unicode character string. The default implementation converts the
   * String to a {@link org.apache.avro.util.Utf8}. Some Encoder implementations
   * may want to do something different as a performance optimization.
   * 
   * @throws AvroTypeException If this is a stateful writer and a char-string is
   *                           not expected
   */
  public void writeString(String str) throws IOException {
    writeString(new Utf8(str));
  }

  /**
   * Write a Unicode character string. If the CharSequence is an
   * {@link org.apache.avro.util.Utf8} it writes this directly, otherwise the
   * CharSequence is converted to a String via toString() and written.
   * 
   * @throws AvroTypeException If this is a stateful writer and a char-string is
   *                           not expected
   */
  public void writeString(CharSequence charSequence) throws IOException {
    if (charSequence instanceof Utf8)
      writeString((Utf8) charSequence);
    else
      writeString(charSequence.toString());
  }

  /**
   * Write a byte string.
   * 
   * @throws AvroTypeException If this is a stateful writer and a byte-string is
   *                           not expected
   */
  public abstract void writeBytes(ByteBuffer bytes) throws IOException;

  /**
   * Write a byte string.
   * 
   * @throws AvroTypeException If this is a stateful writer and a byte-string is
   *                           not expected
   */
  public abstract void writeBytes(byte[] bytes, int start, int len) throws IOException;

  /**
   * Writes a byte string. Equivalent to
   * <tt>writeBytes(bytes, 0, bytes.length)</tt>
   * 
   * @throws IOException
   * @throws AvroTypeException If this is a stateful writer and a byte-string is
   *                           not expected
   */
  public void writeBytes(byte[] bytes) throws IOException {
    writeBytes(bytes, 0, bytes.length);
  }

  /**
   * Writes a fixed size binary object.
   * 
   * @param bytes The contents to write
   * @param start The position within <tt>bytes</tt> where the contents start.
   * @param len   The number of bytes to write.
   * @throws AvroTypeException If this is a stateful writer and a byte-string is
   *                           not expected
   * @throws IOException
   */
  public abstract void writeFixed(byte[] bytes, int start, int len) throws IOException;

  /**
   * A shorthand for <tt>writeFixed(bytes, 0, bytes.length)</tt>
   * 
   * @param bytes
   */
  public void writeFixed(byte[] bytes) throws IOException {
    writeFixed(bytes, 0, bytes.length);
  }

  /** Writes a fixed from a ByteBuffer. */
  public void writeFixed(ByteBuffer bytes) throws IOException {
    int pos = bytes.position();
    int len = bytes.limit() - pos;
    if (bytes.hasArray()) {
      writeFixed(bytes.array(), bytes.arrayOffset() + pos, len);
    } else {
      byte[] b = new byte[len];
      bytes.duplicate().get(b, 0, len);
      writeFixed(b, 0, len);
    }
  }

  /**
   * Writes an enumeration.
   * 
   * @param e
   * @throws AvroTypeException If this is a stateful writer and an enumeration is
   *                           not expected or the <tt>e</tt> is out of range.
   * @throws IOException
   */
  public abstract void writeEnum(int e) throws IOException;

  /**
   * Call this method to start writing an array.
   *
   * When starting to serialize an array, call {@link #writeArrayStart}. Then,
   * before writing any data for any item call {@link #setItemCount} followed by a
   * sequence of {@link #startItem()} and the item itself. The number of
   * {@link #startItem()} should match the number specified in
   * {@link #setItemCount}. When actually writing the data of the item, you can
   * call any {@link Encoder} method (e.g., {@link #writeLong}). When all items of
   * the array have been written, call {@link #writeArrayEnd}.
   *
   * As an example, let's say you want to write an array of records, the record
   * consisting of an Long field and a Boolean field. Your code would look
   * something like this:
   * 
   * <pre>
   * out.writeArrayStart();
   * out.setItemCount(list.size());
   * for (Record r : list) {
   *   out.startItem();
   *   out.writeLong(r.longField);
   *   out.writeBoolean(r.boolField);
   * }
   * out.writeArrayEnd();
   * </pre>
   * 
   * @throws AvroTypeException If this is a stateful writer and an array is not
   *                           expected
   */
  public abstract void writeArrayStart() throws IOException;

  /**
   * Call this method before writing a batch of items in an array or a map. Then
   * for each item, call {@link #startItem()} followed by any of the other write
   * methods of {@link Encoder}. The number of calls to {@link #startItem()} must
   * be equal to the count specified in {@link #setItemCount}. Once a batch is
   * completed you can start another batch with {@link #setItemCount}.
   *
   * @param itemCount The number of {@link #startItem()} calls to follow.
   * @throws IOException
   */
  public abstract void setItemCount(long itemCount) throws IOException;

  /**
   * Start a new item of an array or map. See {@link #writeArrayStart} for usage
   * information.
   * 
   * @throws AvroTypeException If called outside of an array or map context
   */
  public abstract void startItem() throws IOException;

  /**
   * Call this method to finish writing an array. See {@link #writeArrayStart} for
   * usage information.
   *
   * @throws AvroTypeException If items written does not match count provided to
   *                           {@link #writeArrayStart}
   * @throws AvroTypeException If not currently inside an array
   */
  public abstract void writeArrayEnd() throws IOException;

  /**
   * Call this to start a new map. See {@link #writeArrayStart} for details on
   * usage.
   *
   * As an example of usage, let's say you want to write a map of records, the
   * record consisting of an Long field and a Boolean field. Your code would look
   * something like this:
   * 
   * <pre>
   * out.writeMapStart();
   * out.setItemCount(list.size());
   * for (Map.Entry<String, Record> entry : map.entrySet()) {
   *   out.startItem();
   *   out.writeString(entry.getKey());
   *   out.writeLong(entry.getValue().longField);
   *   out.writeBoolean(entry.getValue().boolField);
   * }
   * out.writeMapEnd();
   * </pre>
   * 
   * @throws AvroTypeException If this is a stateful writer and a map is not
   *                           expected
   */
  public abstract void writeMapStart() throws IOException;

  /**
   * Call this method to terminate the inner-most, currently-opened map. See
   * {@link #writeArrayStart} for more details.
   *
   * @throws AvroTypeException If items written does not match count provided to
   *                           {@link #writeMapStart}
   * @throws AvroTypeException If not currently inside a map
   */
  public abstract void writeMapEnd() throws IOException;

  /**
   * Call this method to write the tag of a union.
   *
   * As an example of usage, let's say you want to write a union, whose second
   * branch is a record consisting of an Long field and a Boolean field. Your code
   * would look something like this:
   * 
   * <pre>
   * out.writeIndex(1);
   * out.writeLong(record.longField);
   * out.writeBoolean(record.boolField);
   * </pre>
   * 
   * @throws AvroTypeException If this is a stateful writer and a map is not
   *                           expected
   */
  public abstract void writeIndex(int unionIndex) throws IOException;
}
