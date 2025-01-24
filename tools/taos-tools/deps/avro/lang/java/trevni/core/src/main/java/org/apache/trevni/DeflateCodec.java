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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterOutputStream;

/** Implements DEFLATE (RFC1951) compression and decompression. */
class DeflateCodec extends Codec {
  private ByteArrayOutputStream outputBuffer;
  private Deflater deflater;
  private Inflater inflater;

  @Override
  ByteBuffer compress(ByteBuffer data) throws IOException {
    ByteArrayOutputStream baos = getOutputBuffer(data.remaining());
    try (OutputStream outputStream = new DeflaterOutputStream(baos, getDeflater())) {
      outputStream.write(data.array(), computeOffset(data), data.remaining());
    }
    return ByteBuffer.wrap(baos.toByteArray());
  }

  @Override
  ByteBuffer decompress(ByteBuffer data) throws IOException {
    ByteArrayOutputStream baos = getOutputBuffer(data.remaining());
    try (OutputStream outputStream = new InflaterOutputStream(baos, getInflater())) {
      outputStream.write(data.array(), computeOffset(data), data.remaining());
    }
    return ByteBuffer.wrap(baos.toByteArray());
  }

  private Inflater getInflater() {
    if (null == inflater)
      inflater = new Inflater(true);
    inflater.reset();
    return inflater;
  }

  private Deflater getDeflater() {
    if (null == deflater)
      deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
    deflater.reset();
    return deflater;
  }

  private ByteArrayOutputStream getOutputBuffer(int suggestedLength) {
    if (null == outputBuffer)
      outputBuffer = new ByteArrayOutputStream(suggestedLength);
    outputBuffer.reset();
    return outputBuffer;
  }

}
