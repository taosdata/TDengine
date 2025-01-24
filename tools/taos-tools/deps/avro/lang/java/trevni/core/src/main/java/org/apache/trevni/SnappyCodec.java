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
import java.nio.Buffer;
import java.nio.ByteBuffer;
import org.xerial.snappy.Snappy;

/** Implements <a href="https://code.google.com/p/snappy/">Snappy</a> codec. */
final class SnappyCodec extends Codec {

  @Override
  ByteBuffer compress(ByteBuffer in) throws IOException {
    int offset = computeOffset(in);
    ByteBuffer out = ByteBuffer.allocate(Snappy.maxCompressedLength(in.remaining()));
    int size = Snappy.compress(in.array(), offset, in.remaining(), out.array(), 0);
    ((Buffer) out).limit(size);
    return out;
  }

  @Override
  ByteBuffer decompress(ByteBuffer in) throws IOException {
    int offset = computeOffset(in);
    ByteBuffer out = ByteBuffer.allocate(Snappy.uncompressedLength(in.array(), offset, in.remaining()));
    int size = Snappy.uncompress(in.array(), offset, in.remaining(), out.array(), 0);
    ((Buffer) out).limit(size);
    return out;
  }

}
