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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class TestAllCodecs {
  @Parameterized.Parameters(name = "{index}: codec={0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] { { "bzip2" }, { "null" }, { "snappy" }, { "deflate" }, });
  }

  @Parameterized.Parameter(0)
  public String codec;

  public static Codec getCodec(String name) {
    MetaData m = new MetaData();
    m.put(MetaData.CODEC_KEY, name.getBytes());
    return Codec.get(m);
  }

  @Test
  public void testCodec() throws IOException {
    int inputSize = 500_000;

    byte[] input = generateTestData(inputSize);

    Codec codecInstance = getCodec(codec);
    ByteBuffer inputByteBuffer = ByteBuffer.wrap(input);
    ByteBuffer compressedBuffer = codecInstance.compress(inputByteBuffer);

    int compressedSize = compressedBuffer.remaining();

    // Make sure something returned
    assertTrue(compressedSize > 0);

    // While the compressed size could in many real cases
    // *increase* compared to the input size, our input data
    // is extremely easy to compress and all Avro's compression algorithms
    // should have a compression ratio greater than 1 (except 'null').
    assertTrue(compressedSize < inputSize || codec.equals("null"));

    // Decompress the data
    ByteBuffer decompressedBuffer = codecInstance.decompress(compressedBuffer);

    // Validate the the input and output are equal.
    ((Buffer) inputByteBuffer).rewind();
    assertEquals(decompressedBuffer, inputByteBuffer);
  }

  @Test
  public void testCodecSlice() throws IOException {
    int inputSize = 500_000;
    byte[] input = generateTestData(inputSize);

    Codec codecInstance = getCodec(codec);
    ByteBuffer partialBuffer = ByteBuffer.wrap(input);
    ((Buffer) partialBuffer).position(17);

    ByteBuffer inputByteBuffer = partialBuffer.slice();
    ByteBuffer compressedBuffer = codecInstance.compress(inputByteBuffer);

    int compressedSize = compressedBuffer.remaining();

    // Make sure something returned
    assertTrue(compressedSize > 0);

    // Create a slice from the compressed buffer
    ByteBuffer sliceBuffer = ByteBuffer.allocate(compressedSize + 100);
    ((Buffer) sliceBuffer).position(50);
    sliceBuffer.put(compressedBuffer);
    ((Buffer) sliceBuffer).limit(compressedSize + 50);
    ((Buffer) sliceBuffer).position(50);

    // Decompress the data
    ByteBuffer decompressedBuffer = codecInstance.decompress(sliceBuffer.slice());

    // Validate the the input and output are equal.
    ((Buffer) inputByteBuffer).rewind();
    assertEquals(decompressedBuffer, inputByteBuffer);
  }

  // Generate some test data that will compress easily
  public static byte[] generateTestData(int inputSize) {
    byte[] arr = new byte[inputSize];
    for (int i = 0; i < arr.length; i++) {
      arr[i] = (byte) (65 + i % 10);
    }

    return arr;
  }
}
