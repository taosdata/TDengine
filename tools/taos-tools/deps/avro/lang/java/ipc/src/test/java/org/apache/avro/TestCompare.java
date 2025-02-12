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
package org.apache.avro;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.io.BinaryData;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;

import org.apache.avro.test.TestRecord;
import org.apache.avro.test.Kind;
import org.apache.avro.test.MD5;

public class TestCompare {

  @Test
  public void testNull() throws Exception {
    Schema schema = new Schema.Parser().parse("\"null\"");
    byte[] b = render(null, schema, new GenericDatumWriter<>());
    assertEquals(0, BinaryData.compare(b, 0, b, 0, schema));
  }

  @Test
  public void testBoolean() throws Exception {
    check("\"boolean\"", Boolean.FALSE, Boolean.TRUE);
  }

  @Test
  public void testString() throws Exception {
    check("\"string\"", new Utf8(""), new Utf8("a"));
    check("\"string\"", new Utf8("a"), new Utf8("b"));
    check("\"string\"", new Utf8("a"), new Utf8("ab"));
    check("\"string\"", new Utf8("ab"), new Utf8("b"));
  }

  @Test
  public void testBytes() throws Exception {
    check("\"bytes\"", ByteBuffer.wrap(new byte[] {}), ByteBuffer.wrap(new byte[] { 1 }));
    check("\"bytes\"", ByteBuffer.wrap(new byte[] { 1 }), ByteBuffer.wrap(new byte[] { 2 }));
    check("\"bytes\"", ByteBuffer.wrap(new byte[] { 1, 2 }), ByteBuffer.wrap(new byte[] { 2 }));
  }

  @Test
  public void testInt() throws Exception {
    check("\"int\"", -1, 0);
    check("\"int\"", 0, 1);
  }

  @Test
  public void testLong() throws Exception {
    check("\"long\"", 11L, 12L);
    check("\"long\"", (long) -1, 1L);
  }

  @Test
  public void testFloat() throws Exception {
    check("\"float\"", 1.1f, 1.2f);
    check("\"float\"", (float) -1.1, 1.0f);
  }

  @Test
  public void testDouble() throws Exception {
    check("\"double\"", 1.2, 1.3);
    check("\"double\"", -1.2, 1.3);
  }

  @Test
  public void testArray() throws Exception {
    String json = "{\"type\":\"array\", \"items\": \"long\"}";
    Schema schema = new Schema.Parser().parse(json);
    GenericArray<Long> a1 = new GenericData.Array<>(1, schema);
    a1.add(1L);
    GenericArray<Long> a2 = new GenericData.Array<>(1, schema);
    a2.add(1L);
    a2.add(0L);
    check(json, a1, a2);
  }

  @Test
  public void testRecord() throws Exception {
    String fields = " \"fields\":[" + "{\"name\":\"f\",\"type\":\"int\",\"order\":\"ignore\"},"
        + "{\"name\":\"g\",\"type\":\"int\",\"order\":\"descending\"}," + "{\"name\":\"h\",\"type\":\"int\"}]}";
    String recordJson = "{\"type\":\"record\", \"name\":\"Test\"," + fields;
    Schema schema = new Schema.Parser().parse(recordJson);
    GenericData.Record r1 = new GenericData.Record(schema);
    r1.put("f", 1);
    r1.put("g", 13);
    r1.put("h", 41);
    GenericData.Record r2 = new GenericData.Record(schema);
    r2.put("f", 0);
    r2.put("g", 12);
    r2.put("h", 41);
    check(recordJson, r1, r2);
    r2.put("f", 0);
    r2.put("g", 13);
    r2.put("h", 42);
    check(recordJson, r1, r2);

    String record2Json = "{\"type\":\"record\", \"name\":\"Test2\"," + fields;
    Schema schema2 = new Schema.Parser().parse(record2Json);
    GenericData.Record r3 = new GenericData.Record(schema2);
    r3.put("f", 1);
    r3.put("g", 13);
    r3.put("h", 41);
    assert (!r1.equals(r3)); // same fields, diff name
  }

  @Test
  public void testEnum() throws Exception {
    String json = "{\"type\":\"enum\", \"name\":\"Test\",\"symbols\": [\"A\", \"B\"]}";
    Schema schema = new Schema.Parser().parse(json);
    check(json, new GenericData.EnumSymbol(schema, "A"), new GenericData.EnumSymbol(schema, "B"));
  }

  @Test
  public void testFixed() throws Exception {
    String json = "{\"type\": \"fixed\", \"name\":\"Test\", \"size\": 1}";
    Schema schema = new Schema.Parser().parse(json);
    check(json, new GenericData.Fixed(schema, new byte[] { (byte) 'a' }),
        new GenericData.Fixed(schema, new byte[] { (byte) 'b' }));
  }

  @Test
  public void testUnion() throws Exception {
    check("[\"string\", \"long\"]", new Utf8("a"), new Utf8("b"), false);
    check("[\"string\", \"long\"]", 1L, 2L, false);
    check("[\"string\", \"long\"]", new Utf8("a"), 1L, false);
  }

  @Test
  public void testSpecificRecord() throws Exception {
    TestRecord s1 = new TestRecord();
    TestRecord s2 = new TestRecord();
    s1.setName("foo");
    s1.setKind(Kind.BAZ);
    s1.setHash(new MD5(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5 }));
    s2.setName("bar");
    s2.setKind(Kind.BAR);
    s2.setHash(new MD5(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 6 }));
    Schema schema = SpecificData.get().getSchema(TestRecord.class);

    check(schema, s1, s2, true, new SpecificDatumWriter<>(schema), SpecificData.get());
    s2.setKind(Kind.BAZ);
    check(schema, s1, s2, true, new SpecificDatumWriter<>(schema), SpecificData.get());
  }

  private static <T> void check(String schemaJson, T o1, T o2) throws Exception {
    check(schemaJson, o1, o2, true);
  }

  private static <T> void check(String schemaJson, T o1, T o2, boolean comparable) throws Exception {
    check(new Schema.Parser().parse(schemaJson), o1, o2, comparable, new GenericDatumWriter<>(), GenericData.get());
  }

  private static <T> void check(Schema schema, T o1, T o2, boolean comparable, DatumWriter<T> writer,
      GenericData comparator) throws Exception {

    byte[] b1 = render(o1, schema, writer);
    byte[] b2 = render(o2, schema, writer);
    assertEquals(-1, BinaryData.compare(b1, 0, b2, 0, schema));
    assertEquals(1, BinaryData.compare(b2, 0, b1, 0, schema));
    assertEquals(0, BinaryData.compare(b1, 0, b1, 0, schema));
    assertEquals(0, BinaryData.compare(b2, 0, b2, 0, schema));

    assertEquals(-1, compare(o1, o2, schema, comparable, comparator));
    assertEquals(1, compare(o2, o1, schema, comparable, comparator));
    assertEquals(0, compare(o1, o1, schema, comparable, comparator));
    assertEquals(0, compare(o2, o2, schema, comparable, comparator));

    assert (o1.equals(o1));
    assert (o2.equals(o2));
    assert (!o1.equals(o2));
    assert (!o2.equals(o1));
    assert (!o1.equals(new Object()));
    assert (!o2.equals(new Object()));
    assert (!o1.equals(null));
    assert (!o2.equals(null));

    assert (o1.hashCode() != o2.hashCode());

    // check BinaryData.hashCode against Object.hashCode
    if (schema.getType() != Schema.Type.ENUM) {
      assertEquals(o1.hashCode(), BinaryData.hashCode(b1, 0, b1.length, schema));
      assertEquals(o2.hashCode(), BinaryData.hashCode(b2, 0, b2.length, schema));
    }

    // check BinaryData.hashCode against GenericData.hashCode
    assertEquals(comparator.hashCode(o1, schema), BinaryData.hashCode(b1, 0, b1.length, schema));
    assertEquals(comparator.hashCode(o2, schema), BinaryData.hashCode(b2, 0, b2.length, schema));

  }

  @SuppressWarnings(value = "unchecked")
  private static int compare(Object o1, Object o2, Schema schema, boolean comparable, GenericData comparator) {
    return comparable ? ((Comparable<Object>) o1).compareTo(o2) : comparator.compare(o1, o2, schema);
  }

  private static <T> byte[] render(T datum, Schema schema, DatumWriter<T> writer) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.setSchema(schema);
    Encoder enc = new EncoderFactory().directBinaryEncoder(out, null);
    writer.write(datum, enc);
    enc.flush();
    return out.toByteArray();
  }
}
