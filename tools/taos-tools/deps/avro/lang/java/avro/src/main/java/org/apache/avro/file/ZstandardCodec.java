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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.avro.util.NonCopyingByteArrayOutputStream;
import org.apache.commons.compress.utils.IOUtils;

public class ZstandardCodec extends Codec {
  public final static int DEFAULT_COMPRESSION = 3;
  public final static boolean DEFAULT_USE_BUFFERPOOL = false;
  private static final int DEFAULT_BUFFER_SIZE = 8192;

  static class Option extends CodecFactory {
    private final int compressionLevel;
    private final boolean useChecksum;
    private final boolean useBufferPool;

    Option(int compressionLevel, boolean useChecksum, boolean useBufferPool) {
      this.compressionLevel = compressionLevel;
      this.useChecksum = useChecksum;
      this.useBufferPool = useBufferPool;
    }

    @Override
    protected Codec createInstance() {
      return new ZstandardCodec(compressionLevel, useChecksum, useBufferPool);
    }
  }

  private final int compressionLevel;
  private final boolean useChecksum;
  private final boolean useBufferPool;

  /**
   * Create a ZstandardCodec instance with the given compressionLevel, checksum,
   * and bufferPool option
   **/
  public ZstandardCodec(int compressionLevel, boolean useChecksum, boolean useBufferPool) {
    this.compressionLevel = compressionLevel;
    this.useChecksum = useChecksum;
    this.useBufferPool = useBufferPool;
  }

  @Override
  public String getName() {
    return DataFileConstants.ZSTANDARD_CODEC;
  }

  @Override
  public ByteBuffer compress(ByteBuffer data) throws IOException {
    NonCopyingByteArrayOutputStream baos = new NonCopyingByteArrayOutputStream(DEFAULT_BUFFER_SIZE);
    try (OutputStream outputStream = ZstandardLoader.output(baos, compressionLevel, useChecksum, useBufferPool)) {
      outputStream.write(data.array(), computeOffset(data), data.remaining());
    }
    return baos.asByteBuffer();
  }

  @Override
  public ByteBuffer decompress(ByteBuffer compressedData) throws IOException {
    NonCopyingByteArrayOutputStream baos = new NonCopyingByteArrayOutputStream(DEFAULT_BUFFER_SIZE);
    InputStream bytesIn = new ByteArrayInputStream(compressedData.array(), computeOffset(compressedData),
        compressedData.remaining());
    try (InputStream ios = ZstandardLoader.input(bytesIn, useBufferPool)) {
      IOUtils.copy(ios, baos);
    }
    return baos.asByteBuffer();
  }

  @Override
  public int hashCode() {
    return getName().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return (this == obj) || (obj != null && obj.getClass() == this.getClass());
  }

  @Override
  public String toString() {
    return getName() + "[" + compressionLevel + "]";
  }
}
