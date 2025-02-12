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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.util.ByteBufferInputStream;
import org.apache.avro.util.ByteBufferOutputStream;
import org.apache.avro.util.RandomData;
import org.apache.avro.util.Utf8;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestBinaryDecoder {
  // prime number buffer size so that looping tests hit the buffer edge
  // at different points in the loop.
  DecoderFactory factory = new DecoderFactory().configureDecoderBufferSize(521);
  private boolean useDirect = false;
  static EncoderFactory e_factory = EncoderFactory.get();

  public TestBinaryDecoder(boolean useDirect) {
    this.useDirect = useDirect;
  }

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] { { true }, { false }, });
  }

  private Decoder newDecoderWithNoData() throws IOException {
    return newDecoder(new byte[0]);
  }

  private Decoder newDecoder(byte[] bytes, int start, int len) throws IOException {
    return factory.binaryDecoder(bytes, start, len, null);

  }

  private Decoder newDecoder(InputStream in) {
    if (useDirect) {
      return factory.directBinaryDecoder(in, null);
    } else {
      return factory.binaryDecoder(in, null);
    }
  }

  private Decoder newDecoder(byte[] bytes) throws IOException {
    return factory.binaryDecoder(bytes, null);
  }

  /** Verify EOFException throw at EOF */

  @Test(expected = EOFException.class)
  public void testEOFBoolean() throws IOException {
    newDecoderWithNoData().readBoolean();
  }

  @Test(expected = EOFException.class)
  public void testEOFInt() throws IOException {
    newDecoderWithNoData().readInt();
  }

  @Test(expected = EOFException.class)
  public void testEOFLong() throws IOException {
    newDecoderWithNoData().readLong();
  }

  @Test(expected = EOFException.class)
  public void testEOFFloat() throws IOException {
    newDecoderWithNoData().readFloat();
  }

  @Test(expected = EOFException.class)
  public void testEOFDouble() throws IOException {
    newDecoderWithNoData().readDouble();
  }

  @Test(expected = EOFException.class)
  public void testEOFBytes() throws IOException {
    newDecoderWithNoData().readBytes(null);
  }

  @Test(expected = EOFException.class)
  public void testEOFString() throws IOException {
    newDecoderWithNoData().readString(new Utf8("a"));
  }

  @Test(expected = EOFException.class)
  public void testEOFFixed() throws IOException {
    newDecoderWithNoData().readFixed(new byte[1]);
  }

  @Test(expected = EOFException.class)
  public void testEOFEnum() throws IOException {
    newDecoderWithNoData().readEnum();
  }

  @Test
  public void testReuse() throws IOException {
    ByteBufferOutputStream bbo1 = new ByteBufferOutputStream();
    ByteBufferOutputStream bbo2 = new ByteBufferOutputStream();
    byte[] b1 = new byte[] { 1, 2 };

    BinaryEncoder e1 = e_factory.binaryEncoder(bbo1, null);
    e1.writeBytes(b1);
    e1.flush();

    BinaryEncoder e2 = e_factory.binaryEncoder(bbo2, null);
    e2.writeBytes(b1);
    e2.flush();

    DirectBinaryDecoder d = new DirectBinaryDecoder(new ByteBufferInputStream(bbo1.getBufferList()));
    ByteBuffer bb1 = d.readBytes(null);
    Assert.assertEquals(b1.length, bb1.limit() - bb1.position());

    d.configure(new ByteBufferInputStream(bbo2.getBufferList()));
    ByteBuffer bb2 = d.readBytes(null);
    Assert.assertEquals(b1.length, bb2.limit() - bb2.position());

  }

  private static byte[] data = null;
  private static Schema schema = null;
  private static int count = 200;
  private static ArrayList<Object> records = new ArrayList<>(count);

  @BeforeClass
  public static void generateData() throws IOException {
    int seed = (int) System.currentTimeMillis();
    // note some tests (testSkipping) rely on this explicitly
    String jsonSchema = "{\"type\": \"record\", \"name\": \"Test\", \"fields\": ["
        + "{\"name\":\"intField\", \"type\":\"int\"}," + "{\"name\":\"bytesField\", \"type\":\"bytes\"},"
        + "{\"name\":\"booleanField\", \"type\":\"boolean\"}," + "{\"name\":\"stringField\", \"type\":\"string\"},"
        + "{\"name\":\"floatField\", \"type\":\"float\"}," + "{\"name\":\"doubleField\", \"type\":\"double\"},"
        + "{\"name\":\"arrayField\", \"type\": " + "{\"type\":\"array\", \"items\":\"boolean\"}},"
        + "{\"name\":\"longField\", \"type\":\"long\"}]}";
    schema = new Schema.Parser().parse(jsonSchema);
    GenericDatumWriter<Object> writer = new GenericDatumWriter<>();
    writer.setSchema(schema);
    ByteArrayOutputStream baos = new ByteArrayOutputStream(8192);
    BinaryEncoder encoder = e_factory.binaryEncoder(baos, null);

    for (Object datum : new RandomData(schema, count, seed)) {
      writer.write(datum, encoder);
      records.add(datum);
    }
    encoder.flush();
    data = baos.toByteArray();
  }

  @Test
  public void testDecodeFromSources() throws IOException {
    GenericDatumReader<Object> reader = new GenericDatumReader<>();
    reader.setSchema(schema);

    ByteArrayInputStream is = new ByteArrayInputStream(data);
    ByteArrayInputStream is2 = new ByteArrayInputStream(data);
    ByteArrayInputStream is3 = new ByteArrayInputStream(data);

    Decoder fromInputStream = newDecoder(is);
    Decoder fromArray = newDecoder(data);

    byte[] data2 = new byte[data.length + 30];
    Arrays.fill(data2, (byte) 0xff);
    System.arraycopy(data, 0, data2, 15, data.length);

    Decoder fromOffsetArray = newDecoder(data2, 15, data.length);

    BinaryDecoder initOnInputStream = factory.binaryDecoder(new byte[50], 0, 30, null);
    initOnInputStream = factory.binaryDecoder(is2, initOnInputStream);
    BinaryDecoder initOnArray = factory.binaryDecoder(is3, null);
    initOnArray = factory.binaryDecoder(data, 0, data.length, initOnArray);

    for (Object datum : records) {
      Assert.assertEquals("InputStream based BinaryDecoder result does not match", datum,
          reader.read(null, fromInputStream));
      Assert.assertEquals("Array based BinaryDecoder result does not match", datum, reader.read(null, fromArray));
      Assert.assertEquals("offset Array based BinaryDecoder result does not match", datum,
          reader.read(null, fromOffsetArray));
      Assert.assertEquals("InputStream initialized BinaryDecoder result does not match", datum,
          reader.read(null, initOnInputStream));
      Assert.assertEquals("Array initialized BinaryDecoder result does not match", datum,
          reader.read(null, initOnArray));
    }
  }

  @Test
  public void testInputStreamProxy() throws IOException {
    Decoder d = newDecoder(data);
    if (d instanceof BinaryDecoder) {
      BinaryDecoder bd = (BinaryDecoder) d;
      InputStream test = bd.inputStream();
      InputStream check = new ByteArrayInputStream(data);
      validateInputStreamReads(test, check);
      bd = factory.binaryDecoder(data, bd);
      test = bd.inputStream();
      check = new ByteArrayInputStream(data);
      validateInputStreamSkips(test, check);
      // with input stream sources
      bd = factory.binaryDecoder(new ByteArrayInputStream(data), bd);
      test = bd.inputStream();
      check = new ByteArrayInputStream(data);
      validateInputStreamReads(test, check);
      bd = factory.binaryDecoder(new ByteArrayInputStream(data), bd);
      test = bd.inputStream();
      check = new ByteArrayInputStream(data);
      validateInputStreamSkips(test, check);
    }
  }

  @Test
  public void testInputStreamProxyDetached() throws IOException {
    Decoder d = newDecoder(data);
    if (d instanceof BinaryDecoder) {
      BinaryDecoder bd = (BinaryDecoder) d;
      InputStream test = bd.inputStream();
      InputStream check = new ByteArrayInputStream(data);
      // detach input stream and decoder from old source
      factory.binaryDecoder(new byte[56], null);
      InputStream bad = bd.inputStream();
      InputStream check2 = new ByteArrayInputStream(data);
      validateInputStreamReads(test, check);
      Assert.assertFalse(bad.read() == check2.read());
    }
  }

  @Test
  public void testInputStreamPartiallyUsed() throws IOException {
    BinaryDecoder bd = factory.binaryDecoder(new ByteArrayInputStream(data), null);
    InputStream test = bd.inputStream();
    InputStream check = new ByteArrayInputStream(data);
    // triggers buffer fill if unused and tests isEnd()
    try {
      Assert.assertFalse(bd.isEnd());
    } catch (UnsupportedOperationException e) {
      // this is ok if its a DirectBinaryDecoder.
      if (bd.getClass() != DirectBinaryDecoder.class) {
        throw e;
      }
    }
    bd.readFloat(); // use data, and otherwise trigger buffer fill
    check.skip(4); // skip the same # of bytes here
    validateInputStreamReads(test, check);
  }

  private void validateInputStreamReads(InputStream test, InputStream check) throws IOException {
    byte[] bt = new byte[7];
    byte[] bc = new byte[7];
    while (true) {
      int t = test.read();
      int c = check.read();
      Assert.assertEquals(c, t);
      if (-1 == t)
        break;
      t = test.read(bt);
      c = check.read(bc);
      Assert.assertEquals(c, t);
      Assert.assertArrayEquals(bt, bc);
      if (-1 == t)
        break;
      t = test.read(bt, 1, 4);
      c = check.read(bc, 1, 4);
      Assert.assertEquals(c, t);
      Assert.assertArrayEquals(bt, bc);
      if (-1 == t)
        break;
    }
    Assert.assertEquals(0, test.skip(5));
    Assert.assertEquals(0, test.available());
    Assert.assertFalse(test.getClass() != ByteArrayInputStream.class && test.markSupported());
    test.close();
  }

  private void validateInputStreamSkips(InputStream test, InputStream check) throws IOException {
    while (true) {
      long t2 = test.skip(19);
      long c2 = check.skip(19);
      Assert.assertEquals(c2, t2);
      if (0 == t2)
        break;
    }
    Assert.assertEquals(-1, test.read());
  }

  @Test
  public void testBadIntEncoding() throws IOException {
    byte[] badint = new byte[5];
    Arrays.fill(badint, (byte) 0xff);
    Decoder bd = factory.binaryDecoder(badint, null);
    String message = "";
    try {
      bd.readInt();
    } catch (IOException ioe) {
      message = ioe.getMessage();
    }
    Assert.assertEquals("Invalid int encoding", message);
  }

  @Test
  public void testBadLongEncoding() throws IOException {
    byte[] badint = new byte[10];
    Arrays.fill(badint, (byte) 0xff);
    Decoder bd = factory.binaryDecoder(badint, null);
    String message = "";
    try {
      bd.readLong();
    } catch (IOException ioe) {
      message = ioe.getMessage();
    }
    Assert.assertEquals("Invalid long encoding", message);
  }

  @Test
  public void testNegativeStringLength() throws IOException {
    byte[] bad = new byte[] { (byte) 1 };
    Decoder bd = factory.binaryDecoder(bad, null);

    Assert.assertThrows("Malformed data. Length is negative: -1", AvroRuntimeException.class, bd::readString);
  }

  @Test
  public void testStringMaxArraySize() throws IOException {
    byte[] bad = new byte[10];
    BinaryData.encodeLong(BinaryDecoder.MAX_ARRAY_SIZE + 1, bad, 0);
    Decoder bd = factory.binaryDecoder(bad, null);

    Assert.assertThrows("Cannot read strings longer than " + BinaryDecoder.MAX_ARRAY_SIZE + " bytes",
        UnsupportedOperationException.class, bd::readString);
  }

  @Test
  public void testNegativeBytesLength() throws IOException {
    byte[] bad = new byte[] { (byte) 1 };
    Decoder bd = factory.binaryDecoder(bad, null);

    Assert.assertThrows("Malformed data. Length is negative: -1", AvroRuntimeException.class, () -> bd.readBytes(null));
  }

  @Test
  public void testBytesMaxArraySize() throws IOException {
    byte[] bad = new byte[10];
    BinaryData.encodeLong(BinaryDecoder.MAX_ARRAY_SIZE + 1, bad, 0);
    Decoder bd = factory.binaryDecoder(bad, null);

    Assert.assertThrows("Cannot read arrays longer than " + BinaryDecoder.MAX_ARRAY_SIZE + " bytes",
        UnsupportedOperationException.class, () -> bd.readBytes(null));
  }

  @Test
  public void testBytesMaxLengthProperty() throws IOException {
    int maxLength = 128;
    byte[] bad = new byte[10];
    BinaryData.encodeLong(maxLength + 1, bad, 0);
    try {
      System.setProperty("org.apache.avro.limits.bytes.maxLength", Long.toString(maxLength));
      Decoder bd = factory.binaryDecoder(bad, null);

      Assert.assertThrows("Bytes length " + (maxLength + 1) + " exceeds maximum allowed", AvroRuntimeException.class,
          () -> bd.readBytes(null));
    } finally {
      System.clearProperty("org.apache.avro.limits.bytes.maxLength");
    }
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testLongLengthEncoding() throws IOException {
    // Size equivalent to Integer.MAX_VALUE + 1
    byte[] bad = new byte[] { (byte) -128, (byte) -128, (byte) -128, (byte) -128, (byte) 16 };
    Decoder bd = factory.binaryDecoder(bad, null);
    bd.readString();
  }

  @Test(expected = EOFException.class)
  public void testIntTooShort() throws IOException {
    byte[] badint = new byte[4];
    Arrays.fill(badint, (byte) 0xff);
    newDecoder(badint).readInt();
  }

  @Test(expected = EOFException.class)
  public void testLongTooShort() throws IOException {
    byte[] badint = new byte[9];
    Arrays.fill(badint, (byte) 0xff);
    newDecoder(badint).readLong();
  }

  @Test(expected = EOFException.class)
  public void testFloatTooShort() throws IOException {
    byte[] badint = new byte[3];
    Arrays.fill(badint, (byte) 0xff);
    newDecoder(badint).readInt();
  }

  @Test(expected = EOFException.class)
  public void testDoubleTooShort() throws IOException {
    byte[] badint = new byte[7];
    Arrays.fill(badint, (byte) 0xff);
    newDecoder(badint).readLong();
  }

  @Test
  public void testSkipping() throws IOException {
    Decoder d = newDecoder(data);
    skipGenerated(d);
    if (d instanceof BinaryDecoder) {
      BinaryDecoder bd = (BinaryDecoder) d;
      try {
        Assert.assertTrue(bd.isEnd());
      } catch (UnsupportedOperationException e) {
        // this is ok if its a DirectBinaryDecoder.
        if (bd.getClass() != DirectBinaryDecoder.class) {
          throw e;
        }
      }
      bd = factory.binaryDecoder(new ByteArrayInputStream(data), bd);
      skipGenerated(bd);
      try {
        Assert.assertTrue(bd.isEnd());
      } catch (UnsupportedOperationException e) {
        // this is ok if its a DirectBinaryDecoder.
        if (bd.getClass() != DirectBinaryDecoder.class) {
          throw e;
        }
      }
    }
  }

  private void skipGenerated(Decoder bd) throws IOException {
    for (int i = 0; i < records.size(); i++) {
      bd.readInt();
      bd.skipBytes();
      bd.skipFixed(1);
      bd.skipString();
      bd.skipFixed(4);
      bd.skipFixed(8);
      long leftover = bd.skipArray();
      // booleans are one byte, array trailer is one byte
      bd.skipFixed((int) leftover + 1);
      bd.skipFixed(0);
      bd.readLong();
    }
    EOFException eof = null;
    try {
      bd.skipFixed(4);
    } catch (EOFException e) {
      eof = e;
    }
    Assert.assertTrue(null != eof);
  }

  @Test(expected = EOFException.class)
  public void testEOF() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Encoder e = EncoderFactory.get().binaryEncoder(baos, null);
    e.writeLong(0x10000000000000L);
    e.flush();

    Decoder d = newDecoder(new ByteArrayInputStream(baos.toByteArray()));
    Assert.assertEquals(0x10000000000000L, d.readLong());
    d.readInt();
  }

}
