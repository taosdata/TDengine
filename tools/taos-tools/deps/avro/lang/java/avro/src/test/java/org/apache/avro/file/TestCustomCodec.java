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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.file.codec.CustomCodec;
import org.junit.Test;

public class TestCustomCodec {

  @Test
  public void testCustomCodec() {
    CustomCodec customCodec = new CustomCodec();
    Codec snappyCodec = new SnappyCodec.Option().createInstance();
    assertTrue(customCodec.equals(new CustomCodec()));
    assertFalse(customCodec.equals(snappyCodec));

    String testString = "Testing 123";
    ByteBuffer original = ByteBuffer.allocate(testString.getBytes(UTF_8).length);
    original.put(testString.getBytes(UTF_8));
    original.rewind();
    ByteBuffer decompressed = null;
    try {
      ByteBuffer compressed = customCodec.compress(original);
      compressed.rewind();
      decompressed = customCodec.decompress(compressed);
    } catch (IOException e) {
      e.printStackTrace();
    }

    assertEquals(testString, new String(decompressed.array(), UTF_8));

  }

}
