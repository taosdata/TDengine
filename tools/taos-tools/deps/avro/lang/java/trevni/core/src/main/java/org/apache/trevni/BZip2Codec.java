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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;

public class BZip2Codec extends Codec {

  private ByteArrayOutputStream outputBuffer;
  public static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

  @Override
  ByteBuffer compress(ByteBuffer uncompressedData) throws IOException {
    ByteArrayOutputStream baos = getOutputBuffer(uncompressedData.remaining());

    try (BZip2CompressorOutputStream outputStream = new BZip2CompressorOutputStream(baos)) {
      outputStream.write(uncompressedData.array(), computeOffset(uncompressedData), uncompressedData.remaining());
    }

    return ByteBuffer.wrap(baos.toByteArray());
  }

  @Override
  ByteBuffer decompress(ByteBuffer compressedData) throws IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(compressedData.array(), computeOffset(compressedData),
        compressedData.remaining());
    try (BZip2CompressorInputStream inputStream = new BZip2CompressorInputStream(bais)) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();

      byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];

      int readCount = -1;

      while ((readCount = inputStream.read(buffer, compressedData.position(), buffer.length)) > 0) {
        baos.write(buffer, 0, readCount);
      }

      return ByteBuffer.wrap(baos.toByteArray());
    }
  }

  private ByteArrayOutputStream getOutputBuffer(int suggestedLength) {
    if (null == outputBuffer)
      outputBuffer = new ByteArrayOutputStream(suggestedLength);
    outputBuffer.reset();
    return outputBuffer;
  }

}
