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
package org.apache.avro.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.avro.util.Utf8;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBinaryEncoderFidelity {

  static byte[] legacydata;
  static byte[] complexdata;
  EncoderFactory factory = EncoderFactory.get();

  public static void generateData(Encoder e, boolean useReadOnlyByteBuffer) throws IOException {
    // generate a bunch of data that should test the bounds of a BinaryEncoder
    Random r = new Random(665321);
    e.writeNull();
    e.writeBoolean(true);
    e.writeBoolean(false);
    byte[] bytes = new byte[10];
    ByteBuffer bb;
    if (useReadOnlyByteBuffer) {
      bb = ByteBuffer.wrap(bytes, 4, 4).asReadOnlyBuffer();
    } else {
      bb = ByteBuffer.wrap(bytes, 4, 4);
    }
    r.nextBytes(bytes);
    e.writeBytes(bytes);
    e.writeBytes(new byte[0]);
    e.writeBytes(bytes, 3, 3);
    e.writeBytes(new byte[0], 0, 0);
    e.writeBytes(ByteBuffer.wrap(bytes, 2, 2));
    e.writeBytes(bb);
    e.writeBytes(bb);
    e.writeDouble(0.0);
    e.writeDouble(-0.0);
    e.writeDouble(Double.NaN);
    e.writeDouble(r.nextDouble());
    e.writeDouble(Double.NEGATIVE_INFINITY);
    e.writeEnum(65);
    e.writeFixed(bytes);
    e.writeFixed(bytes, 7, 2);
    e.writeFloat(1.0f);
    e.writeFloat(r.nextFloat());
    e.writeFloat(Float.POSITIVE_INFINITY);
    e.writeFloat(Float.MIN_NORMAL);
    e.writeIndex(-2);
    e.writeInt(0);
    e.writeInt(-1);
    e.writeInt(1);
    e.writeInt(0x40);
    e.writeInt(-0x41);
    e.writeInt(0x2000);
    e.writeInt(-0x2001);
    e.writeInt(0x80000);
    e.writeInt(-0x80001);
    e.writeInt(0x4000000);
    e.writeInt(-0x4000001);
    e.writeInt(r.nextInt());
    e.writeInt(r.nextInt());
    e.writeInt(Integer.MAX_VALUE);
    e.writeInt(Integer.MIN_VALUE);
    e.writeLong(0);
    e.writeLong(-1);
    e.writeLong(1);
    e.writeLong(0x40);
    e.writeLong(-0x41);
    e.writeLong(0x2000);
    e.writeLong(-0x2001);
    e.writeLong(0x80000);
    e.writeLong(-0x80001);
    e.writeLong(0x4000000);
    e.writeLong(-0x4000001);
    e.writeLong(0x200000000L);
    e.writeLong(-0x200000001L);
    e.writeLong(0x10000000000L);
    e.writeLong(-0x10000000001L);
    e.writeLong(0x800000000000L);
    e.writeLong(-0x800000000001L);
    e.writeLong(0x40000000000000L);
    e.writeLong(-0x40000000000001L);
    e.writeLong(0x2000000000000000L);
    e.writeLong(-0x2000000000000001L);
    e.writeLong(r.nextLong());
    e.writeLong(r.nextLong());
    e.writeLong(Long.MAX_VALUE);
    e.writeLong(Long.MIN_VALUE);
    e.writeString(new StringBuilder("StringBuilder\u00A2"));
    e.writeString("String\u20AC");
    e.writeString("");
    e.writeString(new Utf8("Utf8\uD834\uDD1E"));
    if (e instanceof BinaryEncoder) {
      int count = ((BinaryEncoder) e).bytesBuffered();
      System.out.println(e.getClass().getSimpleName() + " buffered: " + count);
    }
    e.flush();
  }

  static void generateComplexData(Encoder e) throws IOException {
    e.writeArrayStart();
    e.setItemCount(1);
    e.startItem();
    e.writeInt(1);
    e.writeArrayEnd();
    e.writeMapStart();
    e.setItemCount(2);
    e.startItem();
    e.writeString("foo");
    e.writeInt(-1);
    e.writeDouble(33.3);
    e.startItem();
    e.writeString("bar");
    e.writeInt(1);
    e.writeDouble(-33.3);
    e.writeMapEnd();
    e.flush();
  }

  @BeforeClass
  public static void generateLegacyData() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Encoder e = new LegacyBinaryEncoder(baos);
    generateData(e, false);
    legacydata = baos.toByteArray();
    baos.reset();
    generateComplexData(e);
    complexdata = baos.toByteArray();
  }

  @Test
  public void testBinaryEncoder() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    BinaryEncoder e = factory.binaryEncoder(baos, null);
    generateData(e, true);
    byte[] result = baos.toByteArray();
    Assert.assertEquals(legacydata.length, result.length);
    Assert.assertArrayEquals(legacydata, result);
    baos.reset();
    generateComplexData(e);
    byte[] result2 = baos.toByteArray();
    Assert.assertEquals(complexdata.length, result2.length);
    Assert.assertArrayEquals(complexdata, result2);
  }

  @Test
  public void testDirectBinaryEncoder() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    BinaryEncoder e = factory.directBinaryEncoder(baos, null);
    generateData(e, true);
    byte[] result = baos.toByteArray();
    Assert.assertEquals(legacydata.length, result.length);
    Assert.assertArrayEquals(legacydata, result);
    baos.reset();
    generateComplexData(e);
    byte[] result2 = baos.toByteArray();
    Assert.assertEquals(complexdata.length, result2.length);
    Assert.assertArrayEquals(complexdata, result2);
  }

  @Test
  public void testBlockingBinaryEncoder() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    BinaryEncoder e = factory.blockingBinaryEncoder(baos, null);
    generateData(e, true);
    byte[] result = baos.toByteArray();
    Assert.assertEquals(legacydata.length, result.length);
    Assert.assertArrayEquals(legacydata, result);
    baos.reset();
    generateComplexData(e);
    byte[] result2 = baos.toByteArray();
    // blocking will cause different length, should be two bytes larger
    Assert.assertEquals(complexdata.length + 2, result2.length);
    // the first byte is the array start, with the count of items negative
    Assert.assertEquals(complexdata[0] >>> 1, result2[0]);
  }
}
