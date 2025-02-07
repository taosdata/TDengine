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
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorOutputStream;
import org.apache.commons.compress.utils.IOUtils;

/** * Implements xz compression and decompression. */
public class XZCodec extends Codec {
  public final static int DEFAULT_COMPRESSION = 6;
  private static final int DEFAULT_BUFFER_SIZE = 8192;

  static class Option extends CodecFactory {
    private int compressionLevel;

    Option(int compressionLevel) {
      this.compressionLevel = compressionLevel;
    }

    @Override
    protected Codec createInstance() {
      return new XZCodec(compressionLevel);
    }
  }

  private int compressionLevel;

  public XZCodec(int compressionLevel) {
    this.compressionLevel = compressionLevel;
  }

  @Override
  public String getName() {
    return DataFileConstants.XZ_CODEC;
  }

  @Override
  public ByteBuffer compress(ByteBuffer data) throws IOException {
    NonCopyingByteArrayOutputStream baos = new NonCopyingByteArrayOutputStream(DEFAULT_BUFFER_SIZE);
    try (OutputStream outputStream = new XZCompressorOutputStream(baos, compressionLevel)) {
      outputStream.write(data.array(), computeOffset(data), data.remaining());
    }
    return baos.asByteBuffer();
  }

  @Override
  public ByteBuffer decompress(ByteBuffer data) throws IOException {
    NonCopyingByteArrayOutputStream baos = new NonCopyingByteArrayOutputStream(DEFAULT_BUFFER_SIZE);
    InputStream bytesIn = new ByteArrayInputStream(data.array(), computeOffset(data), data.remaining());

    try (InputStream ios = new XZCompressorInputStream(bytesIn)) {
      IOUtils.copy(ios, baos);
    }
    return baos.asByteBuffer();
  }

  @Override
  public int hashCode() {
    return compressionLevel;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null || obj.getClass() != getClass())
      return false;
    XZCodec other = (XZCodec) obj;
    return (this.compressionLevel == other.compressionLevel);
  }

  @Override
  public String toString() {
    return getName() + "-" + compressionLevel;
  }
}
