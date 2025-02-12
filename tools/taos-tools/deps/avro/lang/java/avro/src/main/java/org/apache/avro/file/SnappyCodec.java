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
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import org.xerial.snappy.Snappy;

/** * Implements Snappy compression and decompression. */
public class SnappyCodec extends Codec {
  private CRC32 crc32 = new CRC32();

  static class Option extends CodecFactory {
    static {
      // if snappy isn't available, this will throw an exception which we
      // can catch so we can avoid registering this codec
      Snappy.getNativeLibraryVersion();
    }

    @Override
    protected Codec createInstance() {
      return new SnappyCodec();
    }
  }

  private SnappyCodec() {
  }

  @Override
  public String getName() {
    return DataFileConstants.SNAPPY_CODEC;
  }

  @Override
  public ByteBuffer compress(ByteBuffer in) throws IOException {
    int offset = computeOffset(in);
    ByteBuffer out = ByteBuffer.allocate(Snappy.maxCompressedLength(in.remaining()) + 4);
    int size = Snappy.compress(in.array(), offset, in.remaining(), out.array(), 0);
    crc32.reset();
    crc32.update(in.array(), offset, in.remaining());
    out.putInt(size, (int) crc32.getValue());

    ((Buffer) out).limit(size + 4);

    return out;
  }

  @Override
  public ByteBuffer decompress(ByteBuffer in) throws IOException {
    int offset = computeOffset(in);
    ByteBuffer out = ByteBuffer.allocate(Snappy.uncompressedLength(in.array(), offset, in.remaining() - 4));
    int size = Snappy.uncompress(in.array(), offset, in.remaining() - 4, out.array(), 0);
    ((Buffer) out).limit(size);

    crc32.reset();
    crc32.update(out.array(), 0, size);
    if (in.getInt(((Buffer) in).limit() - 4) != (int) crc32.getValue())
      throw new IOException("Checksum failure");

    return out;
  }

  @Override
  public int hashCode() {
    return getName().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    return obj != null && obj.getClass() == getClass();
  }
}
