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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterOutputStream;

import org.apache.avro.util.NonCopyingByteArrayOutputStream;

/**
 * Implements DEFLATE (RFC1951) compression and decompression.
 *
 * Note that there is a distinction between RFC1951 (deflate) and RFC1950
 * (zlib). zlib adds an extra 2-byte header at the front, and a 4-byte checksum
 * at the end. The code here, by passing "true" as the "nowrap" option to
 * {@link Inflater} and {@link Deflater}, is using RFC1951.
 */
public class DeflateCodec extends Codec {

  private static final int DEFAULT_BUFFER_SIZE = 8192;

  static class Option extends CodecFactory {
    private int compressionLevel;

    Option(int compressionLevel) {
      this.compressionLevel = compressionLevel;
    }

    @Override
    protected Codec createInstance() {
      return new DeflateCodec(compressionLevel);
    }
  }

  private Deflater deflater;
  private Inflater inflater;
  // currently only do 'nowrap' -- RFC 1951, not zlib
  private boolean nowrap = true;
  private int compressionLevel;

  public DeflateCodec(int compressionLevel) {
    this.compressionLevel = compressionLevel;
  }

  @Override
  public String getName() {
    return DataFileConstants.DEFLATE_CODEC;
  }

  @Override
  public ByteBuffer compress(ByteBuffer data) throws IOException {
    NonCopyingByteArrayOutputStream baos = new NonCopyingByteArrayOutputStream(DEFAULT_BUFFER_SIZE);
    try (OutputStream outputStream = new DeflaterOutputStream(baos, getDeflater())) {
      outputStream.write(data.array(), computeOffset(data), data.remaining());
    }
    return baos.asByteBuffer();
  }

  @Override
  public ByteBuffer decompress(ByteBuffer data) throws IOException {
    NonCopyingByteArrayOutputStream baos = new NonCopyingByteArrayOutputStream(DEFAULT_BUFFER_SIZE);
    try (OutputStream outputStream = new InflaterOutputStream(baos, getInflater())) {
      outputStream.write(data.array(), computeOffset(data), data.remaining());
    }
    return baos.asByteBuffer();
  }

  // get and initialize the inflater for use.
  private Inflater getInflater() {
    if (null == inflater) {
      inflater = new Inflater(nowrap);
    } else {
      inflater.reset();
    }
    return inflater;
  }

  // get and initialize the deflater for use.
  private Deflater getDeflater() {
    if (null == deflater) {
      deflater = new Deflater(compressionLevel, nowrap);
    } else {
      deflater.reset();
    }
    return deflater;
  }

  @Override
  public int hashCode() {
    return nowrap ? 0 : 1;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null || obj.getClass() != getClass())
      return false;
    DeflateCodec other = (DeflateCodec) obj;
    return (this.nowrap == other.nowrap);
  }

  @Override
  public String toString() {
    return getName() + "-" + compressionLevel;
  }
}
