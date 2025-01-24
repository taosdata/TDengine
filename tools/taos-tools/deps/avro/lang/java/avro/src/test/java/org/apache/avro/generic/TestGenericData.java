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
package org.apache.avro.generic;

import static org.apache.avro.TestCircularReferences.Reference;
import static org.apache.avro.TestCircularReferences.Referenceable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.TestCircularReferences.ReferenceManager;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.io.BinaryData;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.junit.Test;

public class TestGenericData {

  @Test(expected = AvroRuntimeException.class)
  public void testrecordConstructorNullSchema() throws Exception {
    new GenericData.Record(null);
  }

  @Test(expected = AvroRuntimeException.class)
  public void testrecordConstructorWrongSchema() throws Exception {
    new GenericData.Record(Schema.create(Schema.Type.INT));
  }

  @Test(expected = AvroRuntimeException.class)
  public void testArrayConstructorNullSchema() throws Exception {
    new GenericData.Array<>(1, null);
  }

  @Test(expected = AvroRuntimeException.class)
  public void testArrayConstructorWrongSchema() throws Exception {
    new GenericData.Array<>(1, Schema.create(Schema.Type.INT));
  }

  @Test(expected = AvroRuntimeException.class)
  public void testRecordCreateEmptySchema() throws Exception {
    Schema s = Schema.createRecord("schemaName", "schemaDoc", "namespace", false);
    new GenericData.Record(s);
  }

  @Test(expected = AvroRuntimeException.class)
  public void testGetEmptySchemaFields() throws Exception {
    Schema s = Schema.createRecord("schemaName", "schemaDoc", "namespace", false);
    s.getFields();
  }

  @Test(expected = AvroRuntimeException.class)
  public void testGetEmptySchemaField() throws Exception {
    Schema s = Schema.createRecord("schemaName", "schemaDoc", "namespace", false);
    s.getField("foo");
  }

  @Test(expected = AvroRuntimeException.class)
  public void testRecordPutInvalidField() throws Exception {
    Schema s = Schema.createRecord("schemaName", "schemaDoc", "namespace", false);
    List<Schema.Field> fields = new ArrayList<>();
    fields.add(new Schema.Field("someFieldName", s, "docs", null));
    s.setFields(fields);
    Record r = new GenericData.Record(s);
    r.put("invalidFieldName", "someValue");
  }

  @Test
  /** Make sure that even with nulls, hashCode() doesn't throw NPE. */
  public void testHashCode() {
    GenericData.get().hashCode(null, Schema.create(Type.NULL));
    GenericData.get().hashCode(null,
        Schema.createUnion(Arrays.asList(Schema.create(Type.BOOLEAN), Schema.create(Type.STRING))));
    List<CharSequence> stuff = new ArrayList<>();
    stuff.add("string");
    Schema schema = recordSchema();
    GenericRecord r = new GenericData.Record(schema);
    r.put(0, stuff);
    GenericData.get().hashCode(r, schema);
  }

  @Test
  public void testEquals() {
    Schema s = recordSchema();
    GenericRecord r0 = new GenericData.Record(s);
    GenericRecord r1 = new GenericData.Record(s);
    GenericRecord r2 = new GenericData.Record(s);
    Collection<CharSequence> l0 = new ArrayDeque<>();
    List<CharSequence> l1 = new ArrayList<>();
    GenericArray<CharSequence> l2 = new GenericData.Array<>(1, s.getFields().get(0).schema());
    String foo = "foo";
    l0.add(new StringBuilder(foo));
    l1.add(foo);
    l2.add(new Utf8(foo));
    r0.put(0, l0);
    r1.put(0, l1);
    r2.put(0, l2);
    assertEquals(r0, r1);
    assertEquals(r0, r2);
    assertEquals(r1, r2);
  }

  private Schema recordSchema() {
    List<Field> fields = new ArrayList<>();
    fields.add(new Field("anArray", Schema.createArray(Schema.create(Type.STRING)), null, null));
    Schema schema = Schema.createRecord("arrayFoo", "test", "mytest", false);
    schema.setFields(fields);

    return schema;
  }

  @Test
  public void testEquals2() {
    Schema schema1 = Schema.createRecord("r", null, "x", false);
    List<Field> fields1 = new ArrayList<>();
    fields1.add(new Field("a", Schema.create(Schema.Type.STRING), null, null, Field.Order.IGNORE));
    schema1.setFields(fields1);

    // only differs in field order
    Schema schema2 = Schema.createRecord("r", null, "x", false);
    List<Field> fields2 = new ArrayList<>();
    fields2.add(new Field("a", Schema.create(Schema.Type.STRING), null, null, Field.Order.ASCENDING));
    schema2.setFields(fields2);

    GenericRecord record1 = new GenericData.Record(schema1);
    record1.put("a", "1");

    GenericRecord record2 = new GenericData.Record(schema2);
    record2.put("a", "2");

    assertFalse(record2.equals(record1));
    assertFalse(record1.equals(record2));
  }

  @Test(expected = AvroRuntimeException.class)
  public void testRecordGetFieldDoesntExist() throws Exception {
    Schema schema = Schema.createRecord("test", "doc", "test", false, Collections.EMPTY_LIST);
    GenericData.Record record = new GenericData.Record(schema);
    record.get("does not exist");
  }

  @Test
  public void testArrayReversal() {
    Schema schema = Schema.createArray(Schema.create(Schema.Type.INT));
    GenericArray<Integer> forward = new GenericData.Array<>(10, schema);
    GenericArray<Integer> backward = new GenericData.Array<>(10, schema);
    for (int i = 0; i <= 9; i++) {
      forward.add(i);
    }
    for (int i = 9; i >= 0; i--) {
      backward.add(i);
    }
    forward.reverse();
    assertTrue(forward.equals(backward));
  }

  @Test
  public void testArrayListInterface() {
    Schema schema = Schema.createArray(Schema.create(Schema.Type.INT));
    GenericArray<Integer> array = new GenericData.Array<>(1, schema);
    array.add(99);
    assertEquals(Integer.valueOf(99), array.get(0));
    List<Integer> list = new ArrayList<>();
    list.add(99);
    assertEquals(array, list);
    assertEquals(list, array);
    assertEquals(list.hashCode(), array.hashCode());
    try {
      array.get(2);
      fail("Expected IndexOutOfBoundsException getting index 2");
    } catch (IndexOutOfBoundsException e) {
    }
    array.clear();
    assertEquals(0, array.size());
    try {
      array.get(0);
      fail("Expected IndexOutOfBoundsException getting index 0 after clear()");
    } catch (IndexOutOfBoundsException e) {
    }

  }

  @Test
  public void testArrayAddAtLocation() {
    Schema schema = Schema.createArray(Schema.create(Schema.Type.INT));
    GenericArray<Integer> array = new GenericData.Array<>(6, schema);
    array.clear();
    for (int i = 0; i < 5; ++i)
      array.add(i);
    assertEquals(5, array.size());
    array.add(0, 6);
    assertEquals(Integer.valueOf(6), array.get(0));
    assertEquals(6, array.size());
    assertEquals(Integer.valueOf(0), array.get(1));
    assertEquals(Integer.valueOf(4), array.get(5));
    array.add(6, 7);
    assertEquals(Integer.valueOf(7), array.get(6));
    assertEquals(7, array.size());
    assertEquals(Integer.valueOf(6), array.get(0));
    assertEquals(Integer.valueOf(4), array.get(5));
    array.add(1, 8);
    assertEquals(Integer.valueOf(8), array.get(1));
    assertEquals(Integer.valueOf(0), array.get(2));
    assertEquals(Integer.valueOf(6), array.get(0));
    assertEquals(8, array.size());
    try {
      array.get(9);
      fail("Expected IndexOutOfBoundsException after adding elements");
    } catch (IndexOutOfBoundsException e) {
    }
  }

  @Test
  public void testArrayRemove() {
    Schema schema = Schema.createArray(Schema.create(Schema.Type.INT));
    GenericArray<Integer> array = new GenericData.Array<>(10, schema);
    array.clear();
    for (int i = 0; i < 10; ++i)
      array.add(i);
    assertEquals(10, array.size());
    assertEquals(Integer.valueOf(0), array.get(0));
    assertEquals(Integer.valueOf(9), array.get(9));

    array.remove(0);
    assertEquals(9, array.size());
    assertEquals(Integer.valueOf(1), array.get(0));
    assertEquals(Integer.valueOf(2), array.get(1));
    assertEquals(Integer.valueOf(9), array.get(8));

    // Test boundary errors.
    try {
      array.get(9);
      fail("Expected IndexOutOfBoundsException after removing an element");
    } catch (IndexOutOfBoundsException e) {
    }
    try {
      array.set(9, 99);
      fail("Expected IndexOutOfBoundsException after removing an element");
    } catch (IndexOutOfBoundsException e) {
    }
    try {
      array.remove(9);
      fail("Expected IndexOutOfBoundsException after removing an element");
    } catch (IndexOutOfBoundsException e) {
    }

    // Test that we can still remove for properly sized arrays, and the rval
    assertEquals(Integer.valueOf(9), array.remove(8));
    assertEquals(8, array.size());

    // Test insertion after remove
    array.add(88);
    assertEquals(Integer.valueOf(88), array.get(8));
  }

  @Test
  public void testArraySet() {
    Schema schema = Schema.createArray(Schema.create(Schema.Type.INT));
    GenericArray<Integer> array = new GenericData.Array<>(10, schema);
    array.clear();
    for (int i = 0; i < 10; ++i)
      array.add(i);
    assertEquals(10, array.size());
    assertEquals(Integer.valueOf(0), array.get(0));
    assertEquals(Integer.valueOf(5), array.get(5));

    assertEquals(Integer.valueOf(5), array.set(5, 55));
    assertEquals(10, array.size());
    assertEquals(Integer.valueOf(55), array.get(5));
  }

  @Test
  public void testToStringIsJson() throws JsonParseException, IOException {
    Field stringField = new Field("string", Schema.create(Type.STRING), null, null);
    Field enumField = new Field("enum", Schema.createEnum("my_enum", "doc", null, Arrays.asList("a", "b", "c")), null,
        null);
    Schema schema = Schema.createRecord("my_record", "doc", "mytest", false);
    schema.setFields(Arrays.asList(stringField, enumField));

    GenericRecord r = new GenericData.Record(schema);
    // \u2013 is EN DASH
    r.put(stringField.name(), "hello\nthere\"\tyou\u2013}");
    r.put(enumField.name(), new GenericData.EnumSymbol(enumField.schema(), "a"));

    String json = r.toString();
    JsonFactory factory = new JsonFactory();
    JsonParser parser = factory.createParser(json);
    ObjectMapper mapper = new ObjectMapper();

    // will throw exception if string is not parsable json
    mapper.readTree(parser);
  }

  @Test
  public void testMapWithNonStringKeyToStringIsJson() throws Exception {
    Schema intMapSchema = new Schema.Parser()
        .parse("{\"type\": \"map\", \"values\": \"string\", \"java-key-class\" : \"java.lang.Integer\"}");
    Field intMapField = new Field("intMap", Schema.createMap(intMapSchema), null, null);
    Schema decMapSchema = new Schema.Parser()
        .parse("{\"type\": \"map\", \"values\": \"string\", \"java-key-class\" : \"java.math.BigDecimal\"}");
    Field decMapField = new Field("decMap", Schema.createMap(decMapSchema), null, null);
    Schema boolMapSchema = new Schema.Parser()
        .parse("{\"type\": \"map\", \"values\": \"string\", \"java-key-class\" : \"java.lang.Boolean\"}");
    Field boolMapField = new Field("boolMap", Schema.createMap(boolMapSchema), null, null);
    Schema fileMapSchema = new Schema.Parser()
        .parse("{\"type\": \"map\", \"values\": \"string\", \"java-key-class\" : \"java.io.File\"}");
    Field fileMapField = new Field("fileMap", Schema.createMap(fileMapSchema), null, null);
    Schema schema = Schema.createRecord("my_record", "doc", "mytest", false);
    schema.setFields(Arrays.asList(intMapField, decMapField, boolMapField, fileMapField));

    HashMap<Integer, String> intPair = new HashMap<>();
    intPair.put(1, "one");
    intPair.put(2, "two");

    HashMap<java.math.BigDecimal, String> decPair = new HashMap<>();
    decPair.put(java.math.BigDecimal.valueOf(1), "one");
    decPair.put(java.math.BigDecimal.valueOf(2), "two");

    HashMap<Boolean, String> boolPair = new HashMap<>();
    boolPair.put(true, "isTrue");
    boolPair.put(false, "isFalse");
    boolPair.put(null, null);

    HashMap<java.io.File, String> filePair = new HashMap<>();
    java.io.File f = new java.io.File(getClass().getResource("/SchemaBuilder.avsc").toURI());
    filePair.put(f, "File");

    GenericRecord r = new GenericData.Record(schema);
    r.put(intMapField.name(), intPair);
    r.put(decMapField.name(), decPair);
    r.put(boolMapField.name(), boolPair);
    r.put(fileMapField.name(), filePair);

    String json = r.toString();
    JsonFactory factory = new JsonFactory();
    JsonParser parser = factory.createParser(json);
    ObjectMapper mapper = new ObjectMapper();

    // will throw exception if string is not parsable json
    mapper.readTree(parser);
  }

  @Test
  public void testToStringEscapesControlCharsInBytes() throws Exception {
    GenericData data = GenericData.get();
    ByteBuffer bytes = ByteBuffer.wrap(new byte[] { 'a', '\n', 'b' });
    assertEquals("\"a\\nb\"", data.toString(bytes));
    assertEquals("\"a\\nb\"", data.toString(bytes));
  }

  @Test
  public void testToStringEscapesControlCharsInMap() {
    GenericData data = GenericData.get();
    Map<String, String> m = new HashMap<>();
    m.put("a\n\\b", "a\n\\b");
    assertEquals("{\"a\\n\\\\b\": \"a\\n\\\\b\"}", data.toString(m));
  }

  @Test
  public void testToStringFixed() throws Exception {
    GenericData data = GenericData.get();
    assertEquals("[97, 10, 98]",
        data.toString(new GenericData.Fixed(Schema.createFixed("test", null, null, 3), new byte[] { 'a', '\n', 'b' })));
  }

  @Test
  public void testToStringDoesNotEscapeForwardSlash() throws Exception {
    GenericData data = GenericData.get();
    assertEquals("\"/\"", data.toString("/"));
  }

  @Test
  public void testToStringNanInfinity() throws Exception {
    GenericData data = GenericData.get();
    assertEquals("\"Infinity\"", data.toString(Float.POSITIVE_INFINITY));
    assertEquals("\"-Infinity\"", data.toString(Float.NEGATIVE_INFINITY));
    assertEquals("\"NaN\"", data.toString(Float.NaN));
    assertEquals("\"Infinity\"", data.toString(Double.POSITIVE_INFINITY));
    assertEquals("\"-Infinity\"", data.toString(Double.NEGATIVE_INFINITY));
    assertEquals("\"NaN\"", data.toString(Double.NaN));
  }

  @Test
  public void testToStringConvertsDatesAsStrings() throws Exception {
    GenericData data = GenericData.get();
    assertEquals("\"1961-04-12T06:07:10Z\"", data.toString(Instant.parse("1961-04-12T06:07:10Z")));
    assertEquals("\"1961-04-12\"", data.toString(LocalDate.parse("1961-04-12")));
    assertEquals("\"1961-04-12T06:07:10\"", data.toString(LocalDateTime.parse("1961-04-12T06:07:10")));
    assertEquals("\"10:10:10\"", data.toString(LocalTime.parse("10:10:10")));
  }

  @Test
  public void testCompare() {
    // Prepare a schema for testing.
    Field integerField = new Field("test", Schema.create(Type.INT), null, null);
    List<Field> fields = new ArrayList<>();
    fields.add(integerField);
    Schema record = Schema.createRecord("test", null, null, false);
    record.setFields(fields);

    ByteArrayOutputStream b1 = new ByteArrayOutputStream(5);
    ByteArrayOutputStream b2 = new ByteArrayOutputStream(5);
    BinaryEncoder b1Enc = EncoderFactory.get().binaryEncoder(b1, null);
    BinaryEncoder b2Enc = EncoderFactory.get().binaryEncoder(b2, null);
    // Prepare two different datums
    Record testDatum1 = new Record(record);
    testDatum1.put(0, 1);
    Record testDatum2 = new Record(record);
    testDatum2.put(0, 2);
    GenericDatumWriter<Record> gWriter = new GenericDatumWriter<>(record);
    Integer start1 = 0, start2 = 0;
    try {
      // Write two datums in each stream
      // and get the offset length after the first write in each.
      gWriter.write(testDatum1, b1Enc);
      b1Enc.flush();
      start1 = b1.size();
      gWriter.write(testDatum1, b1Enc);
      b1Enc.flush();
      b1.close();
      gWriter.write(testDatum2, b2Enc);
      b2Enc.flush();
      start2 = b2.size();
      gWriter.write(testDatum2, b2Enc);
      b2Enc.flush();
      b2.close();
      // Compare to check if offset-based compare works right.
      assertEquals(-1, BinaryData.compare(b1.toByteArray(), start1, b2.toByteArray(), start2, record));
    } catch (IOException e) {
      fail("IOException while writing records to output stream.");
    }
  }

  @Test
  public void testEnumCompare() {
    Schema s = Schema.createEnum("Kind", null, null, Arrays.asList("Z", "Y", "X"));
    GenericEnumSymbol z = new GenericData.EnumSymbol(s, "Z");
    GenericEnumSymbol z2 = new GenericData.EnumSymbol(s, "Z");
    assertEquals(0, z.compareTo(z2));
    GenericEnumSymbol y = new GenericData.EnumSymbol(s, "Y");
    assertTrue(y.compareTo(z) > 0);
    assertTrue(z.compareTo(y) < 0);
  }

  @Test
  public void testByteBufferDeepCopy() {
    // Test that a deep copy of a byte buffer respects the byte buffer
    // limits and capacity.
    byte[] buffer_value = { 0, 1, 2, 3, 0, 0, 0 };
    ByteBuffer buffer = ByteBuffer.wrap(buffer_value, 1, 4);
    Schema schema = Schema.createRecord("my_record", "doc", "mytest", false);
    Field byte_field = new Field("bytes", Schema.create(Type.BYTES), null, null);
    schema.setFields(Collections.singletonList(byte_field));

    GenericRecord record = new GenericData.Record(schema);
    record.put(byte_field.name(), buffer);

    GenericRecord copy = GenericData.get().deepCopy(schema, record);
    ByteBuffer buffer_copy = (ByteBuffer) copy.get(byte_field.name());

    assertEquals(buffer, buffer_copy);
  }

  @Test
  public void testValidateNullableEnum() {
    List<Schema> unionTypes = new ArrayList<>();
    Schema schema;
    Schema nullSchema = Schema.create(Type.NULL);
    Schema enumSchema = Schema.createEnum("AnEnum", null, null, Arrays.asList("X", "Y", "Z"));
    GenericEnumSymbol w = new GenericData.EnumSymbol(enumSchema, "W");
    GenericEnumSymbol x = new GenericData.EnumSymbol(enumSchema, "X");
    GenericEnumSymbol y = new GenericData.EnumSymbol(enumSchema, "Y");
    GenericEnumSymbol z = new GenericData.EnumSymbol(enumSchema, "Z");

    // null is first
    unionTypes.clear();
    unionTypes.add(nullSchema);
    unionTypes.add(enumSchema);
    schema = Schema.createUnion(unionTypes);

    assertTrue(GenericData.get().validate(schema, z));
    assertTrue(GenericData.get().validate(schema, y));
    assertTrue(GenericData.get().validate(schema, x));
    assertFalse(GenericData.get().validate(schema, w));
    assertTrue(GenericData.get().validate(schema, null));

    // null is last
    unionTypes.clear();
    unionTypes.add(enumSchema);
    unionTypes.add(nullSchema);
    schema = Schema.createUnion(unionTypes);

    assertTrue(GenericData.get().validate(schema, z));
    assertTrue(GenericData.get().validate(schema, y));
    assertTrue(GenericData.get().validate(schema, x));
    assertFalse(GenericData.get().validate(schema, w));
    assertTrue(GenericData.get().validate(schema, null));
  }

  private enum anEnum {
    ONE, TWO, THREE
  };

  @Test
  public void validateRequiresGenericSymbolForEnumSchema() {
    final Schema schema = Schema.createEnum("my_enum", "doc", "namespace", Arrays.asList("ONE", "TWO", "THREE"));
    final GenericData gd = GenericData.get();

    /* positive cases */
    assertTrue(gd.validate(schema, new GenericData.EnumSymbol(schema, "ONE")));
    assertTrue(gd.validate(schema, new GenericData.EnumSymbol(schema, anEnum.ONE)));

    /* negative cases */
    assertFalse("We don't expect GenericData to allow a String datum for an enum schema", gd.validate(schema, "ONE"));
    assertFalse("We don't expect GenericData to allow a Java Enum for an enum schema", gd.validate(schema, anEnum.ONE));
  }

  @Test
  public void testValidateUnion() {
    Schema type1Schema = SchemaBuilder.record("Type1").fields().requiredString("myString").requiredInt("myInt")
        .endRecord();

    Schema type2Schema = SchemaBuilder.record("Type2").fields().requiredString("myString").endRecord();

    Schema unionSchema = SchemaBuilder.unionOf().type(type1Schema).and().type(type2Schema).endUnion();

    GenericRecord record = new GenericData.Record(type2Schema);
    record.put("myString", "myValue");
    assertTrue(GenericData.get().validate(unionSchema, record));
  }

  /*
   * The toString has a detection for circular references to abort. This detection
   * has the risk of detecting that same value as being a circular reference. For
   * Record, Map and Array this is correct, for the rest is is not.
   */
  @Test
  public void testToStringSameValues() throws IOException {
    List<Field> fields = new ArrayList<>();
    fields.add(new Field("nullstring1", Schema.create(Type.STRING), null, null));
    fields.add(new Field("nullstring2", Schema.create(Type.STRING), null, null));

    fields.add(new Field("string1", Schema.create(Type.STRING), null, null));
    fields.add(new Field("string2", Schema.create(Type.STRING), null, null));

    fields.add(new Field("bytes1", Schema.create(Type.BYTES), null, null));
    fields.add(new Field("bytes2", Schema.create(Type.BYTES), null, null));

    fields.add(new Field("int1", Schema.create(Type.INT), null, null));
    fields.add(new Field("int2", Schema.create(Type.INT), null, null));

    fields.add(new Field("long1", Schema.create(Type.LONG), null, null));
    fields.add(new Field("long2", Schema.create(Type.LONG), null, null));

    fields.add(new Field("float1", Schema.create(Type.FLOAT), null, null));
    fields.add(new Field("float2", Schema.create(Type.FLOAT), null, null));

    fields.add(new Field("double1", Schema.create(Type.DOUBLE), null, null));
    fields.add(new Field("double2", Schema.create(Type.DOUBLE), null, null));

    fields.add(new Field("boolean1", Schema.create(Type.BOOLEAN), null, null));
    fields.add(new Field("boolean2", Schema.create(Type.BOOLEAN), null, null));

    List<String> enumValues = new ArrayList<>();
    enumValues.add("One");
    enumValues.add("Two");
    Schema enumSchema = Schema.createEnum("myEnum", null, null, enumValues);
    fields.add(new Field("enum1", enumSchema, null, null));
    fields.add(new Field("enum2", enumSchema, null, null));

    Schema recordSchema = SchemaBuilder.record("aRecord").fields().requiredString("myString").endRecord();
    fields.add(new Field("record1", recordSchema, null, null));
    fields.add(new Field("record2", recordSchema, null, null));

    Schema arraySchema = Schema.createArray(Schema.create(Type.STRING));
    fields.add(new Field("array1", arraySchema, null, null));
    fields.add(new Field("array2", arraySchema, null, null));

    Schema mapSchema = Schema.createMap(Schema.create(Type.STRING));
    fields.add(new Field("map1", mapSchema, null, null));
    fields.add(new Field("map2", mapSchema, null, null));

    Schema schema = Schema.createRecord("Foo", "test", "mytest", false);
    schema.setFields(fields);

    Record testRecord = new Record(schema);

    testRecord.put("nullstring1", null);
    testRecord.put("nullstring2", null);

    String fortyTwo = "42";
    testRecord.put("string1", fortyTwo);
    testRecord.put("string2", fortyTwo);
    testRecord.put("bytes1", 0x42);
    testRecord.put("bytes2", 0x42);
    testRecord.put("int1", 42);
    testRecord.put("int2", 42);
    testRecord.put("long1", 42L);
    testRecord.put("long2", 42L);
    testRecord.put("float1", 42F);
    testRecord.put("float2", 42F);
    testRecord.put("double1", 42D);
    testRecord.put("double2", 42D);
    testRecord.put("boolean1", true);
    testRecord.put("boolean2", true);

    testRecord.put("enum1", "One");
    testRecord.put("enum2", "One");

    GenericRecord record = new GenericData.Record(recordSchema);
    record.put("myString", "42");
    testRecord.put("record1", record);
    testRecord.put("record2", record);

    GenericArray<String> array = new GenericData.Array<>(1, arraySchema);
    array.clear();
    array.add("42");
    testRecord.put("array1", array);
    testRecord.put("array2", array);

    Map<String, String> map = new HashMap<>();
    map.put("42", "42");
    testRecord.put("map1", map);
    testRecord.put("map2", map);

    String testString = testRecord.toString();
    assertFalse("Record with duplicated values results in wrong 'toString()'",
        testString.contains("CIRCULAR REFERENCE"));
  }

  // Test copied from Apache Parquet:
  // org.apache.parquet.avro.TestCircularReferences
  @Test
  public void testToStringRecursive() throws IOException {
    ReferenceManager manager = new ReferenceManager();
    GenericData model = new GenericData();
    model.addLogicalTypeConversion(manager.getTracker());
    model.addLogicalTypeConversion(manager.getHandler());

    Schema parentSchema = Schema.createRecord("Parent", null, null, false);

    Schema placeholderSchema = Schema.createRecord("Placeholder", null, null, false);
    List<Schema.Field> placeholderFields = new ArrayList<>();
    placeholderFields.add( // at least one field is needed to be a valid schema
        new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null));
    placeholderSchema.setFields(placeholderFields);

    Referenceable idRef = new Referenceable("id");

    Schema parentRefSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG),
        idRef.addToSchema(placeholderSchema));

    Reference parentRef = new Reference("parent");

    List<Schema.Field> childFields = new ArrayList<>();
    childFields.add(new Schema.Field("c", Schema.create(Schema.Type.STRING), null, null));
    childFields.add(new Schema.Field("parent", parentRefSchema, null, null));
    Schema childSchema = parentRef.addToSchema(Schema.createRecord("Child", null, null, false, childFields));

    List<Schema.Field> parentFields = new ArrayList<>();
    parentFields.add(new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null));
    parentFields.add(new Schema.Field("p", Schema.create(Schema.Type.STRING), null, null));
    parentFields.add(new Schema.Field("child", childSchema, null, null));
    parentSchema.setFields(parentFields);

    Schema schema = idRef.addToSchema(parentSchema);

    Record parent = new Record(schema);
    parent.put("id", 1L);
    parent.put("p", "parent data!");

    Record child = new Record(childSchema);
    child.put("c", "child data!");
    child.put("parent", parent);

    parent.put("child", child);

    try {
      assertNotNull(parent.toString()); // This should not fail with an infinite recursion (StackOverflowError)
    } catch (StackOverflowError e) {
      fail("StackOverflowError occurred");
    }
  }

  @Test
  /**
   * check that GenericArray.reset() retains reusable elements and that
   * GenericArray.prune() cleans them up properly.
   */
  public void testGenericArrayPeek() {
    Schema elementSchema = SchemaBuilder.record("element").fields().requiredString("value").endRecord();
    Schema arraySchema = Schema.createArray(elementSchema);

    GenericRecord record = new GenericData.Record(elementSchema);
    record.put("value", "string");

    GenericArray<GenericRecord> list = new GenericData.Array<>(1, arraySchema);
    list.add(record);

    list.reset();
    assertTrue(record == list.peek());

    list.prune();
    assertNull(list.peek());
  }

}
