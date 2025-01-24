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

/**
 * Interface for Avro-supported compression codecs for data files.
 *
 * Note that Codec objects may maintain internal state (e.g. buffers) and are
 * not thread safe.
 */
public abstract class Codec {
  /** Name of the codec; written to the file's metadata. */
  public abstract String getName();

  /** Compresses the input data */
  public abstract ByteBuffer compress(ByteBuffer uncompressedData) throws IOException;

  /** Decompress the data */
  public abstract ByteBuffer decompress(ByteBuffer compressedData) throws IOException;

  /**
   * Codecs must implement an equals() method. Two codecs, A and B are equal if:
   * the result of A and B decompressing content compressed by A is the same AND
   * the result of A and B decompressing content compressed by B is the same
   **/
  @Override
  public abstract boolean equals(Object other);

  /**
   * Codecs must implement a hashCode() method that is consistent with equals().
   */
  @Override
  public abstract int hashCode();

  @Override
  public String toString() {
    return getName();
  }

  // Codecs often reference the array inside a ByteBuffer. Compute the offset
  // to the start of data correctly in the case that our ByteBuffer
  // is a slice() of another.
  protected static int computeOffset(ByteBuffer data) {
    return data.arrayOffset() + data.position();
  }
}
