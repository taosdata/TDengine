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
import java.nio.ByteBuffer;

import org.apache.avro.util.Utf8;

/**
 * Low-level support for de-serializing Avro values.
 * <p/>
 * This class has two types of methods. One type of methods support the reading
 * of leaf values (for example, {@link #readLong} and {@link #readString}).
 * <p/>
 * The other type of methods support the reading of maps and arrays. These
 * methods are {@link #readArrayStart}, {@link #arrayNext}, and similar methods
 * for maps). See {@link #readArrayStart} for details on these methods.)
 * <p/>
 * {@link DecoderFactory} contains Decoder construction and configuration
 * facilities.
 * 
 * @see DecoderFactory
 * @see Encoder
 */

public abstract class Decoder {

  /**
   * "Reads" a null value. (Doesn't actually read anything, but advances the state
   * of the parser if the implementation is stateful.)
   * 
   * @throws AvroTypeException If this is a stateful reader and null is not the
   *                           type of the next value to be read
   */
  public abstract void readNull() throws IOException;

  /**
   * Reads a boolean value written by {@link Encoder#writeBoolean}.
   * 
   * @throws AvroTypeException If this is a stateful reader and boolean is not the
   *                           type of the next value to be read
   */

  public abstract boolean readBoolean() throws IOException;

  /**
   * Reads an integer written by {@link Encoder#writeInt}.
   * 
   * @throws AvroTypeException If encoded value is larger than 32-bits
   * @throws AvroTypeException If this is a stateful reader and int is not the
   *                           type of the next value to be read
   */
  public abstract int readInt() throws IOException;

  /**
   * Reads a long written by {@link Encoder#writeLong}.
   * 
   * @throws AvroTypeException If this is a stateful reader and long is not the
   *                           type of the next value to be read
   */
  public abstract long readLong() throws IOException;

  /**
   * Reads a float written by {@link Encoder#writeFloat}.
   * 
   * @throws AvroTypeException If this is a stateful reader and is not the type of
   *                           the next value to be read
   */
  public abstract float readFloat() throws IOException;

  /**
   * Reads a double written by {@link Encoder#writeDouble}.
   * 
   * @throws AvroTypeException If this is a stateful reader and is not the type of
   *                           the next value to be read
   */
  public abstract double readDouble() throws IOException;

  /**
   * Reads a char-string written by {@link Encoder#writeString}.
   * 
   * @throws AvroTypeException If this is a stateful reader and char-string is not
   *                           the type of the next value to be read
   */
  public abstract Utf8 readString(Utf8 old) throws IOException;

  /**
   * Reads a char-string written by {@link Encoder#writeString}.
   * 
   * @throws AvroTypeException If this is a stateful reader and char-string is not
   *                           the type of the next value to be read
   */
  public abstract String readString() throws IOException;

  /**
   * Discards a char-string written by {@link Encoder#writeString}.
   * 
   * @throws AvroTypeException If this is a stateful reader and char-string is not
   *                           the type of the next value to be read
   */
  public abstract void skipString() throws IOException;

  /**
   * Reads a byte-string written by {@link Encoder#writeBytes}. if <tt>old</tt> is
   * not null and has sufficient capacity to take in the bytes being read, the
   * bytes are returned in <tt>old</tt>.
   * 
   * @throws AvroTypeException If this is a stateful reader and byte-string is not
   *                           the type of the next value to be read
   */
  public abstract ByteBuffer readBytes(ByteBuffer old) throws IOException;

  /**
   * Discards a byte-string written by {@link Encoder#writeBytes}.
   * 
   * @throws AvroTypeException If this is a stateful reader and byte-string is not
   *                           the type of the next value to be read
   */
  public abstract void skipBytes() throws IOException;

  /**
   * Reads fixed sized binary object.
   * 
   * @param bytes  The buffer to store the contents being read.
   * @param start  The position where the data needs to be written.
   * @param length The size of the binary object.
   * @throws AvroTypeException If this is a stateful reader and fixed sized binary
   *                           object is not the type of the next value to be read
   *                           or the length is incorrect.
   * @throws IOException
   */
  public abstract void readFixed(byte[] bytes, int start, int length) throws IOException;

  /**
   * A shorthand for <tt>readFixed(bytes, 0, bytes.length)</tt>.
   * 
   * @throws AvroTypeException If this is a stateful reader and fixed sized binary
   *                           object is not the type of the next value to be read
   *                           or the length is incorrect.
   * @throws IOException
   */
  public void readFixed(byte[] bytes) throws IOException {
    readFixed(bytes, 0, bytes.length);
  }

  /**
   * Discards fixed sized binary object.
   * 
   * @param length The size of the binary object to be skipped.
   * @throws AvroTypeException If this is a stateful reader and fixed sized binary
   *                           object is not the type of the next value to be read
   *                           or the length is incorrect.
   * @throws IOException
   */
  public abstract void skipFixed(int length) throws IOException;

  /**
   * Reads an enumeration.
   * 
   * @return The enumeration's value.
   * @throws AvroTypeException If this is a stateful reader and enumeration is not
   *                           the type of the next value to be read.
   * @throws IOException
   */
  public abstract int readEnum() throws IOException;

  /**
   * Reads and returns the size of the first block of an array. If this method
   * returns non-zero, then the caller should read the indicated number of items,
   * and then call {@link #arrayNext} to find out the number of items in the next
   * block. The typical pattern for consuming an array looks like:
   * 
   * <pre>
   *   for(long i = in.readArrayStart(); i != 0; i = in.arrayNext()) {
   *     for (long j = 0; j < i; j++) {
   *       read next element of the array;
   *     }
   *   }
   * </pre>
   * 
   * @throws AvroTypeException If this is a stateful reader and array is not the
   *                           type of the next value to be read
   */
  public abstract long readArrayStart() throws IOException;

  /**
   * Processes the next block of an array and returns the number of items in the
   * block and let's the caller read those items.
   * 
   * @throws AvroTypeException When called outside of an array context
   */
  public abstract long arrayNext() throws IOException;

  /**
   * Used for quickly skipping through an array. Note you can either skip the
   * entire array, or read the entire array (with {@link #readArrayStart}), but
   * you can't mix the two on the same array.
   *
   * This method will skip through as many items as it can, all of them if
   * possible. It will return zero if there are no more items to skip through, or
   * an item count if it needs the client's help in skipping. The typical usage
   * pattern is:
   * 
   * <pre>
   *   for(long i = in.skipArray(); i != 0; i = i.skipArray()) {
   *     for (long j = 0; j < i; j++) {
   *       read and discard the next element of the array;
   *     }
   *   }
   * </pre>
   * 
   * Note that this method can automatically skip through items if a byte-count is
   * found in the underlying data, or if a schema has been provided to the
   * implementation, but otherwise the client will have to skip through items
   * itself.
   *
   * @throws AvroTypeException If this is a stateful reader and array is not the
   *                           type of the next value to be read
   */
  public abstract long skipArray() throws IOException;

  /**
   * Reads and returns the size of the next block of map-entries. Similar to
   * {@link #readArrayStart}.
   *
   * As an example, let's say you want to read a map of records, the record
   * consisting of an Long field and a Boolean field. Your code would look
   * something like this:
   * 
   * <pre>
   * Map<String, Record> m = new HashMap<String, Record>();
   * Record reuse = new Record();
   * for (long i = in.readMapStart(); i != 0; i = in.readMapNext()) {
   *   for (long j = 0; j < i; j++) {
   *     String key = in.readString();
   *     reuse.intField = in.readInt();
   *     reuse.boolField = in.readBoolean();
   *     m.put(key, reuse);
   *   }
   * }
   * </pre>
   * 
   * @throws AvroTypeException If this is a stateful reader and map is not the
   *                           type of the next value to be read
   */
  public abstract long readMapStart() throws IOException;

  /**
   * Processes the next block of map entries and returns the count of them.
   * Similar to {@link #arrayNext}. See {@link #readMapStart} for details.
   * 
   * @throws AvroTypeException When called outside of a map context
   */
  public abstract long mapNext() throws IOException;

  /**
   * Support for quickly skipping through a map similar to {@link #skipArray}.
   *
   * As an example, let's say you want to skip a map of records, the record
   * consisting of an Long field and a Boolean field. Your code would look
   * something like this:
   * 
   * <pre>
   * for (long i = in.skipMap(); i != 0; i = in.skipMap()) {
   *   for (long j = 0; j < i; j++) {
   *     in.skipString(); // Discard key
   *     in.readInt(); // Discard int-field of value
   *     in.readBoolean(); // Discard boolean-field of value
   *   }
   * }
   * </pre>
   * 
   * @throws AvroTypeException If this is a stateful reader and array is not the
   *                           type of the next value to be read
   */

  public abstract long skipMap() throws IOException;

  /**
   * Reads the tag of a union written by {@link Encoder#writeIndex}.
   * 
   * @throws AvroTypeException If this is a stateful reader and union is not the
   *                           type of the next value to be read
   */
  public abstract int readIndex() throws IOException;
}
