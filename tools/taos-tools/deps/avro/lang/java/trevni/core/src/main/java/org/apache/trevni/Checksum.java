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

import java.nio.ByteBuffer;

/** Interface for checksum algorithms. */
abstract class Checksum {

  public static Checksum get(MetaData meta) {
    String name = meta.getChecksum();
    if (name == null || "null".equals(name))
      return new NullChecksum();
    else if ("crc32".equals(name))
      return new Crc32Checksum();
    else
      throw new TrevniRuntimeException("Unknown checksum: " + name);
  }

  /** The number of bytes per checksum. */
  public abstract int size();

  /** Compute a checksum. */
  public abstract ByteBuffer compute(ByteBuffer data);

}
