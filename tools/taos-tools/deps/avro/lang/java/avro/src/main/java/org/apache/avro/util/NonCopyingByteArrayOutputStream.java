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

package org.apache.avro.util;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

/**
 * Utility to make data written to an {@link ByteArrayOutputStream} directly
 * available as a {@link ByteBuffer}.
 */
public class NonCopyingByteArrayOutputStream extends ByteArrayOutputStream {

  /**
   * Creates a new byte array output stream, with a buffer capacity of the
   * specified size, in bytes.
   *
   * @param size the initial size
   * @throws IllegalArgumentException if size is negative
   */
  public NonCopyingByteArrayOutputStream(int size) {
    super(size);
  }

  /**
   * Get the contents of this ByteArrayOutputStream wrapped as a ByteBuffer. This
   * is a shallow copy. Changes to this ByteArrayOutputstream "write through" to
   * the ByteBuffer.
   *
   * @return The contents of this ByteArrayOutputstream wrapped as a ByteBuffer
   */
  public ByteBuffer asByteBuffer() {
    return ByteBuffer.wrap(super.buf, 0, super.count);
  }
}
