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
import java.nio.ByteBuffer;

/** Interface for compression codecs. */
abstract class Codec {

  public static Codec get(MetaData meta) {
    String name = meta.getCodec();
    if (name == null || "null".equals(name))
      return new NullCodec();
    else if ("deflate".equals(name))
      return new DeflateCodec();
    else if ("snappy".equals(name))
      return new SnappyCodec();
    else if ("bzip2".equals(name))
      return new BZip2Codec();
    else
      throw new TrevniRuntimeException("Unknown codec: " + name);
  }

  /** Compress data */
  abstract ByteBuffer compress(ByteBuffer uncompressedData) throws IOException;

  /** Decompress data */
  abstract ByteBuffer decompress(ByteBuffer compressedData) throws IOException;

  // Codecs often reference the array inside a ByteBuffer. Compute the offset
  // to the start of data correctly in the case that our ByteBuffer
  // is a slice() of another.
  protected static int computeOffset(ByteBuffer data) {
    return data.arrayOffset() + data.position();
  }
}
