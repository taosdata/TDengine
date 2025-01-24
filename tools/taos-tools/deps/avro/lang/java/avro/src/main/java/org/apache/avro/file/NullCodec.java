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
package org.apache.avro.file;

import java.io.IOException;
import java.nio.ByteBuffer;

/** Implements "null" (pass through) codec. */
final class NullCodec extends Codec {

  private static final NullCodec INSTANCE = new NullCodec();

  static class Option extends CodecFactory {
    @Override
    protected Codec createInstance() {
      return INSTANCE;
    }
  }

  /** No options available for NullCodec. */
  public static final CodecFactory OPTION = new Option();

  @Override
  public String getName() {
    return DataFileConstants.NULL_CODEC;
  }

  @Override
  public ByteBuffer compress(ByteBuffer buffer) throws IOException {
    return buffer;
  }

  @Override
  public ByteBuffer decompress(ByteBuffer data) throws IOException {
    return data;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other)
      return true;
    return (other != null && other.getClass() == getClass());
  }

  @Override
  public int hashCode() {
    return 2;
  }
}
