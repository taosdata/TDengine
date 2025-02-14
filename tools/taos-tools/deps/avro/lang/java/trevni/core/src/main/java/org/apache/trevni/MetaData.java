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
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.LinkedHashMap;

/** Base class for metadata. */
public class MetaData<T extends MetaData> extends LinkedHashMap<String, byte[]> {

  static final String RESERVED_KEY_PREFIX = "trevni.";

  static final String CODEC_KEY = RESERVED_KEY_PREFIX + "codec";
  static final String CHECKSUM_KEY = RESERVED_KEY_PREFIX + "checksum";

  private MetaData<?> defaults;

  void setDefaults(MetaData defaults) {
    this.defaults = defaults;
  }

  /** Return the compression codec name. */
  public String getCodec() {
    return getString(CODEC_KEY);
  }

  /** Set the compression codec name. */
  public T setCodec(String codec) {
    setReserved(CODEC_KEY, codec);
    return (T) this;
  }

  /** Return the checksum algorithm name. */
  public String getChecksum() {
    return getString(CHECKSUM_KEY);
  }

  /** Set the checksum algorithm name. */
  public T setChecksum(String checksum) {
    setReserved(CHECKSUM_KEY, checksum);
    return (T) this;
  }

  /** Return the value of a metadata property as a String. */
  public String getString(String key) {
    byte[] value = get(key);
    if (value == null && defaults != null)
      value = defaults.get(key);
    if (value == null)
      return null;
    return new String(value, StandardCharsets.UTF_8);
  }

  /** Return the value of a metadata property as a long. */
  public long getLong(String key) {
    return Long.parseLong(getString(key));
  }

  /** Return true iff a key has any value, false if it is not present. */
  public boolean getBoolean(String key) {
    return get(key) != null;
  }

  /** Set a metadata property to a binary value. */
  public T set(String key, byte[] value) {
    if (isReserved(key)) {
      throw new TrevniRuntimeException("Cannot set reserved key: " + key);
    }
    put(key, value);
    return (T) this;
  }

  /** Test if a metadata key is reserved. */
  public static boolean isReserved(String key) {
    return key.startsWith(RESERVED_KEY_PREFIX);
  }

  /** Set a metadata property to a String value. */
  public T set(String key, String value) {
    return set(key, value.getBytes(StandardCharsets.UTF_8));
  }

  T setReserved(String key, String value) {
    put(key, value.getBytes(StandardCharsets.UTF_8));
    return (T) this;
  }

  T setReservedBoolean(String key, boolean value) {
    if (value)
      setReserved(key, "");
    else
      remove(key);
    return (T) this;
  }

  /** Set a metadata property to a long value. */
  public T set(String key, long value) {
    return set(key, Long.toString(value));
  }

  void write(OutputBuffer out) throws IOException {
    out.writeInt(size());
    for (Map.Entry<String, byte[]> e : entrySet()) {
      out.writeString(e.getKey());
      out.writeBytes(e.getValue());
    }
  }

  static void read(InputBuffer in, MetaData<?> metaData) throws IOException {
    int size = in.readInt();
    for (int i = 0; i < size; i++)
      metaData.put(in.readString(), in.readBytes());
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append('{').append(' ');
    for (Map.Entry<String, byte[]> e : entrySet()) {
      builder.append(e.getKey());
      builder.append('=');
      builder.append(new String(e.getValue(), StandardCharsets.ISO_8859_1));
      builder.append(' ');
    }
    builder.append('}');
    return builder.toString();
  }

}
