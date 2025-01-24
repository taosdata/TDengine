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
package org.apache.avro.reflect;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.JsonProperties;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.TestReflect.SampleRecord.AnotherSampleRecord;
import org.apache.avro.util.Utf8;
import org.junit.Test;

public class TestReflect {

  EncoderFactory factory = new EncoderFactory();

  // test primitive type inference
  @Test
  public void testVoid() {
    check(Void.TYPE, "\"null\"");
    check(Void.class, "\"null\"");
  }

  @Test
  public void testBoolean() {
    check(Boolean.TYPE, "\"boolean\"");
    check(Boolean.class, "\"boolean\"");
  }

  @Test
  public void testInt() {
    check(Integer.TYPE, "\"int\"");
    check(Integer.class, "\"int\"");
  }

  @Test
  public void testByte() {
    check(Byte.TYPE, "{\"type\":\"int\",\"java-class\":\"java.lang.Byte\"}");
    check(Byte.class, "{\"type\":\"int\",\"java-class\":\"java.lang.Byte\"}");
  }

  @Test
  public void testShort() {
    check(Short.TYPE, "{\"type\":\"int\",\"java-class\":\"java.lang.Short\"}");
    check(Short.class, "{\"type\":\"int\",\"java-class\":\"java.lang.Short\"}");
  }

  @Test
  public void testChar() {
    check(Character.TYPE, "{\"type\":\"int\",\"java-class\":\"java.lang.Character\"}");
    check(Character.class, "{\"type\":\"int\",\"java-class\":\"java.lang.Character\"}");
  }

  @Test
  public void testLong() {
    check(Long.TYPE, "\"long\"");
    check(Long.class, "\"long\"");
  }

  @Test
  public void testFloat() {
    check(Float.TYPE, "\"float\"");
    check(Float.class, "\"float\"");
  }

  @Test
  public void testDouble() {
    check(Double.TYPE, "\"double\"");
    check(Double.class, "\"double\"");
  }

  @Test
  public void testString() {
    check("Foo", "\"string\"");
  }

  @Test
  public void testBytes() {
    check(ByteBuffer.allocate(0), "\"bytes\"");
    check(new byte[0], "{\"type\":\"bytes\",\"java-class\":\"[B\"}");
  }

  @Test
  public void testUnionWithCollection() {
    Schema s = new Schema.Parser().parse("[\"null\", {\"type\":\"array\",\"items\":\"float\"}]");
    GenericData data = ReflectData.get();
    assertEquals(1, data.resolveUnion(s, new ArrayList<Float>()));
  }

  @Test
  public void testUnionWithMap() {
    Schema s = new Schema.Parser().parse("[\"null\", {\"type\":\"map\",\"values\":\"float\"}]");
    GenericData data = ReflectData.get();
    assertEquals(1, data.resolveUnion(s, new HashMap<String, Float>()));
  }

  @Test
  public void testUnionWithMapWithUtf8Keys() {
    Schema s = new Schema.Parser().parse("[\"null\", {\"type\":\"map\",\"values\":\"float\"}]");
    GenericData data = ReflectData.get();
    HashMap<Utf8, Float> map = new HashMap<>();
    map.put(new Utf8("foo"), 1.0f);
    assertEquals(1, data.resolveUnion(s, map));
  }

  @Test
  public void testUnionWithFixed() {
    Schema s = new Schema.Parser().parse("[\"null\", {\"type\":\"fixed\",\"name\":\"f\",\"size\":1}]");
    Schema f = new Schema.Parser().parse("{\"type\":\"fixed\",\"name\":\"f\",\"size\":1}");
    GenericData data = ReflectData.get();
    assertEquals(1, data.resolveUnion(s, new GenericData.Fixed(f)));
  }

  @Test
  public void testUnionWithEnum() {
    Schema s = new Schema.Parser().parse("[\"null\", {\"type\":\"enum\",\"name\":\"E\",\"namespace\":"
        + "\"org.apache.avro.reflect.TestReflect\",\"symbols\":[\"A\",\"B\"]}]");
    GenericData data = ReflectData.get();
    assertEquals(1, data.resolveUnion(s, E.A));
  }

  @Test
  public void testUnionWithBytes() {
    Schema s = new Schema.Parser().parse("[\"null\", \"bytes\"]");
    GenericData data = ReflectData.get();
    assertEquals(1, data.resolveUnion(s, ByteBuffer.wrap(new byte[] { 1 })));
  }

  // test map, array and list type inference
  public static class R1 {
    private Map<String, String> mapField = new HashMap<>();
    private String[] arrayField = new String[] { "foo" };
    private List<String> listField = new ArrayList<>();

    {
      mapField.put("foo", "bar");
      listField.add("foo");
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof R1))
        return false;
      R1 that = (R1) o;
      return mapField.equals(that.mapField) && Arrays.equals(this.arrayField, that.arrayField)
          && listField.equals(that.listField);
    }
  }

  @Test
  public void testMap() throws Exception {
    check(R1.class.getDeclaredField("mapField").getGenericType(), "{\"type\":\"map\",\"values\":\"string\"}");
  }

  @Test
  public void testArray() throws Exception {
    check(R1.class.getDeclaredField("arrayField").getGenericType(),
        "{\"type\":\"array\",\"items\":\"string\",\"java-class\":\"[Ljava.lang.String;\"}");
  }

  @Test
  public void testList() throws Exception {
    check(R1.class.getDeclaredField("listField").getGenericType(),
        "{\"type\":\"array\",\"items\":\"string\"" + ",\"java-class\":\"java.util.List\"}");
  }

  @Test
  public void testR1() throws Exception {
    checkReadWrite(new R1());
  }

  // test record, array and list i/o
  public static class R2 {
    private String[] arrayField;
    private Collection<String> collectionField;

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof R2))
        return false;
      R2 that = (R2) o;
      return Arrays.equals(this.arrayField, that.arrayField) && collectionField.equals(that.collectionField);
    }
  }

  @Test
  public void testR2() throws Exception {
    R2 r2 = new R2();
    r2.arrayField = new String[] { "foo" };
    r2.collectionField = new ArrayList<>();
    r2.collectionField.add("foo");
    checkReadWrite(r2);
  }

  // test array i/o of unboxed type
  public static class R3 {
    private int[] intArray;

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof R3))
        return false;
      R3 that = (R3) o;
      return Arrays.equals(this.intArray, that.intArray);
    }
  }

  @Test
  public void testR3() throws Exception {
    R3 r3 = new R3();
    r3.intArray = new int[] { 1 };
    checkReadWrite(r3);
  }

  // test inherited fields & short datatype
  public static class R4 {
    public short value;
    public short[] shorts;
    public byte b;
    public char c;

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof R4))
        return false;
      R4 that = (R4) o;
      return this.value == that.value && Arrays.equals(this.shorts, that.shorts) && this.b == that.b
          && this.c == that.c;
    }
  }

  public static class R5 extends R4 {
  }

  @Test
  public void testR5() throws Exception {
    R5 r5 = new R5();
    r5.value = 1;
    r5.shorts = new short[] { 3, 255, 256, Short.MAX_VALUE, Short.MIN_VALUE };
    r5.b = 99;
    r5.c = 'a';
    checkReadWrite(r5);
  }

  // test union annotation on a class
  @Union({ R7.class, R8.class })
  public static class R6 {
  }

  public static class R7 extends R6 {
    public int value;

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof R7))
        return false;
      return this.value == ((R7) o).value;
    }
  }

  public static class R8 extends R6 {
    public float value;

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof R8))
        return false;
      return this.value == ((R8) o).value;
    }
  }

  // test arrays of union annotated class
  public static class R9 {
    public R6[] r6s;

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof R9))
        return false;
      return Arrays.equals(this.r6s, ((R9) o).r6s);
    }
  }

  @Test
  public void testR6() throws Exception {
    R7 r7 = new R7();
    r7.value = 1;
    checkReadWrite(r7, ReflectData.get().getSchema(R6.class));
    R8 r8 = new R8();
    r8.value = 1;
    checkReadWrite(r8, ReflectData.get().getSchema(R6.class));
    R9 r9 = new R9();
    r9.r6s = new R6[] { r7, r8 };
    checkReadWrite(r9, ReflectData.get().getSchema(R9.class));
  }

  // test union in fields
  public static class R9_1 {
    @Union({ Void.class, R7.class, R8.class })
    public Object value;

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof R9_1))
        return false;
      if (this.value == null)
        return ((R9_1) o).value == null;
      return this.value.equals(((R9_1) o).value);
    }
  }

  @Test
  public void testR6_1() throws Exception {
    R7 r7 = new R7();
    r7.value = 1;
    checkReadWrite(r7, ReflectData.get().getSchema(R6.class));
    R8 r8 = new R8();
    r8.value = 1;
    checkReadWrite(r8, ReflectData.get().getSchema(R6.class));
    R9_1 r9_1 = new R9_1();
    r9_1.value = null;
    checkReadWrite(r9_1, ReflectData.get().getSchema(R9_1.class));
    r9_1.value = r7;
    checkReadWrite(r9_1, ReflectData.get().getSchema(R9_1.class));
    r9_1.value = r8;
    checkReadWrite(r9_1, ReflectData.get().getSchema(R9_1.class));
  }

  // test union annotation on methods and parameters
  public static interface P0 {
    @Union({ Void.class, String.class })
    String foo(@Union({ Void.class, String.class }) String s);
  }

  @Test
  public void testP0() throws Exception {
    Protocol p0 = ReflectData.get().getProtocol(P0.class);
    Protocol.Message message = p0.getMessages().get("foo");
    // check response schema is union
    Schema response = message.getResponse();
    assertEquals(Schema.Type.UNION, response.getType());
    assertEquals(Schema.Type.NULL, response.getTypes().get(0).getType());
    assertEquals(Schema.Type.STRING, response.getTypes().get(1).getType());
    // check request schema is union
    Schema request = message.getRequest();
    Field field = request.getField("s");
    assertNotNull("field 's' should not be null", field);
    Schema param = field.schema();
    assertEquals(Schema.Type.UNION, param.getType());
    assertEquals(Schema.Type.NULL, param.getTypes().get(0).getType());
    assertEquals(Schema.Type.STRING, param.getTypes().get(1).getType());
    // check union erasure
    assertEquals(String.class, ReflectData.get().getClass(response));
    assertEquals(String.class, ReflectData.get().getClass(param));
  }

  // test Stringable annotation
  @Stringable
  public static class R10 {
    private String text;

    public R10(String text) {
      this.text = text;
    }

    @Override
    public String toString() {
      return text;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof R10))
        return false;
      return this.text.equals(((R10) o).text);
    }
  }

  @Test
  public void testR10() throws Exception {
    Schema r10Schema = ReflectData.get().getSchema(R10.class);
    assertEquals(Schema.Type.STRING, r10Schema.getType());
    assertEquals(R10.class.getName(), r10Schema.getProp("java-class"));
    checkReadWrite(new R10("foo"), r10Schema);
  }

  // test Nullable annotation on field
  public static class R11 {
    @Nullable
    private String text;

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof R11))
        return false;
      R11 that = (R11) o;
      if (this.text == null)
        return that.text == null;
      return this.text.equals(that.text);
    }
  }

  @Test
  public void testR11() throws Exception {
    Schema r11Record = ReflectData.get().getSchema(R11.class);
    assertEquals(Schema.Type.RECORD, r11Record.getType());
    Field r11Field = r11Record.getField("text");
    assertEquals(JsonProperties.NULL_VALUE, r11Field.defaultVal());
    Schema r11FieldSchema = r11Field.schema();
    assertEquals(Schema.Type.UNION, r11FieldSchema.getType());
    assertEquals(Schema.Type.NULL, r11FieldSchema.getTypes().get(0).getType());
    Schema r11String = r11FieldSchema.getTypes().get(1);
    assertEquals(Schema.Type.STRING, r11String.getType());
    R11 r11 = new R11();
    checkReadWrite(r11, r11Record);
    r11.text = "foo";
    checkReadWrite(r11, r11Record);
  }

  // test nullable annotation on methods and parameters
  public static interface P1 {
    @Nullable
    String foo(@Nullable String s);
  }

  @Test
  public void testP1() throws Exception {
    Protocol p1 = ReflectData.get().getProtocol(P1.class);
    Protocol.Message message = p1.getMessages().get("foo");
    // check response schema is union
    Schema response = message.getResponse();
    assertEquals(Schema.Type.UNION, response.getType());
    assertEquals(Schema.Type.NULL, response.getTypes().get(0).getType());
    assertEquals(Schema.Type.STRING, response.getTypes().get(1).getType());
    // check request schema is union
    Schema request = message.getRequest();
    Field field = request.getField("s");
    assertNotNull("field 's' should not be null", field);
    Schema param = field.schema();
    assertEquals(Schema.Type.UNION, param.getType());
    assertEquals(Schema.Type.NULL, param.getTypes().get(0).getType());
    assertEquals(Schema.Type.STRING, param.getTypes().get(1).getType());
    // check union erasure
    assertEquals(String.class, ReflectData.get().getClass(response));
    assertEquals(String.class, ReflectData.get().getClass(param));
  }

  // test AvroSchema annotation
  public static class R12 { // fields
    @AvroSchema("\"int\"")
    Object x;

    @AvroSchema("{\"type\":\"array\",\"items\":[\"null\",\"string\"]}")
    List<String> strings;
  }

  @Test
  public void testR12() throws Exception {
    Schema s = ReflectData.get().getSchema(R12.class);
    assertEquals(Schema.Type.INT, s.getField("x").schema().getType());
    assertEquals(new Schema.Parser().parse("{\"type\":\"array\",\"items\":[\"null\",\"string\"]}"),
        s.getField("strings").schema());
  }

  @AvroSchema("\"null\"") // record
  public class R13 {
  }

  @Test
  public void testR13() throws Exception {
    Schema s = ReflectData.get().getSchema(R13.class);
    assertEquals(Schema.Type.NULL, s.getType());
  }

  public interface P4 {
    @AvroSchema("\"int\"") // message value
    Object foo(@AvroSchema("\"int\"") Object x); // message param
  }

  @Test
  public void testP4() throws Exception {
    Protocol p = ReflectData.get().getProtocol(P4.class);
    Protocol.Message message = p.getMessages().get("foo");
    assertEquals(Schema.Type.INT, message.getResponse().getType());
    Field field = message.getRequest().getField("x");
    assertEquals(Schema.Type.INT, field.schema().getType());
  }

  // test error
  @SuppressWarnings("serial")
  public static class E1 extends Exception {
  }

  public static interface P2 {
    void error() throws E1;
  }

  @Test
  public void testP2() throws Exception {
    Schema e1 = ReflectData.get().getSchema(E1.class);
    assertEquals(Schema.Type.RECORD, e1.getType());
    assertTrue(e1.isError());
    Field message = e1.getField("detailMessage");
    assertNotNull("field 'detailMessage' should not be null", message);
    Schema messageSchema = message.schema();
    assertEquals(Schema.Type.UNION, messageSchema.getType());
    assertEquals(Schema.Type.NULL, messageSchema.getTypes().get(0).getType());
    assertEquals(Schema.Type.STRING, messageSchema.getTypes().get(1).getType());

    Protocol p2 = ReflectData.get().getProtocol(P2.class);
    Protocol.Message m = p2.getMessages().get("error");
    // check error schema is union
    Schema response = m.getErrors();
    assertEquals(Schema.Type.UNION, response.getType());
    assertEquals(Schema.Type.STRING, response.getTypes().get(0).getType());
    assertEquals(e1, response.getTypes().get(1));
  }

  @Test
  public void testNoPackage() throws Exception {
    Class<?> noPackage = Class.forName("NoPackage");
    Schema s = ReflectData.get().getSchema(noPackage);
    assertEquals(noPackage.getName(), ReflectData.getClassName(s));
  }

  void checkReadWrite(Object object) throws Exception {
    checkReadWrite(object, ReflectData.get().getSchema(object.getClass()));
  }

  void checkReadWrite(Object object, Schema s) throws Exception {
    ReflectDatumWriter<Object> writer = new ReflectDatumWriter<>(s);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.write(object, factory.directBinaryEncoder(out, null));
    ReflectDatumReader<Object> reader = new ReflectDatumReader<>(s);
    Object after = reader.read(null, DecoderFactory.get().binaryDecoder(out.toByteArray(), null));
    assertEquals(object, after);

    // check reflective setField works for records
    if (s.getType().equals(Schema.Type.RECORD)) {
      Object copy = object.getClass().getDeclaredConstructor().newInstance();
      for (Field f : s.getFields()) {
        Object val = ReflectData.get().getField(object, f.name(), f.pos());
        ReflectData.get().setField(copy, f.name(), f.pos(), val);
      }
      assertEquals("setField", object, copy);
    }
  }

  public static enum E {
    A, B
  };

  @Test
  public void testEnum() throws Exception {
    check(E.class, "{\"type\":\"enum\",\"name\":\"E\",\"namespace\":"
        + "\"org.apache.avro.reflect.TestReflect\",\"symbols\":[\"A\",\"B\"]}");
  }

  public static class R {
    int a;
    long b;
  }

  @Test
  public void testRecord() throws Exception {
    check(R.class,
        "{\"type\":\"record\",\"name\":\"R\",\"namespace\":" + "\"org.apache.avro.reflect.TestReflect\",\"fields\":["
            + "{\"name\":\"a\",\"type\":\"int\"}," + "{\"name\":\"b\",\"type\":\"long\"}]}");
  }

  public static class RAvroIgnore {
    @AvroIgnore
    int a;
  }

  @Test
  public void testAnnotationAvroIgnore() throws Exception {
    check(RAvroIgnore.class, "{\"type\":\"record\",\"name\":\"RAvroIgnore\",\"namespace\":"
        + "\"org.apache.avro.reflect.TestReflect\",\"fields\":[]}");
  }

  @AvroMeta(key = "X", value = "Y")
  public static class RAvroMeta {
    @AvroMeta(key = "K", value = "V")
    int a;
  }

  @Test
  public void testAnnotationAvroMeta() throws Exception {
    check(RAvroMeta.class,
        "{\"type\":\"record\",\"name\":\"RAvroMeta\",\"namespace\":"
            + "\"org.apache.avro.reflect.TestReflect\",\"fields\":[" + "{\"name\":\"a\",\"type\":\"int\",\"K\":\"V\"}]"
            + ",\"X\":\"Y\"}");
  }

  @AvroMeta(key = "X", value = "Y")
  @AvroMeta(key = "A", value = "B")
  public static class RAvroMultiMeta {
    @AvroMeta(key = "K", value = "V")
    @AvroMeta(key = "L", value = "W")
    int a;
  }

  @Test
  public void testAnnotationMultiAvroMeta() {
    check(RAvroMultiMeta.class,
        "{\"type\":\"record\",\"name\":\"RAvroMultiMeta\",\"namespace\":"
            + "\"org.apache.avro.reflect.TestReflect\",\"fields\":["
            + "{\"name\":\"a\",\"type\":\"int\",\"K\":\"V\",\"L\":\"W\"}]" + ",\"X\":\"Y\",\"A\":\"B\"}");
  }

  public static class RAvroDuplicateFieldMeta {
    @AvroMeta(key = "K", value = "V")
    @AvroMeta(key = "K", value = "W")
    int a;
  }

  @Test(expected = AvroTypeException.class)
  public void testAnnotationDuplicateFieldAvroMeta() {
    ReflectData.get().getSchema(RAvroDuplicateFieldMeta.class);
  }

  @AvroMeta(key = "K", value = "V")
  @AvroMeta(key = "K", value = "W")
  public static class RAvroDuplicateTypeMeta {
    int a;
  }

  @Test(expected = AvroTypeException.class)
  public void testAnnotationDuplicateTypeAvroMeta() {
    ReflectData.get().getSchema(RAvroDuplicateTypeMeta.class);
  }

  public static class RAvroName {
    @AvroName("b")
    int a;
  }

  @Test
  public void testAnnotationAvroName() throws Exception {
    check(RAvroName.class, "{\"type\":\"record\",\"name\":\"RAvroName\",\"namespace\":"
        + "\"org.apache.avro.reflect.TestReflect\",\"fields\":[" + "{\"name\":\"b\",\"type\":\"int\"}]}");
  }

  public static class RAvroNameCollide {
    @AvroName("b")
    int a;
    int b;
  }

  @Test(expected = Exception.class)
  public void testAnnotationAvroNameCollide() throws Exception {
    check(RAvroNameCollide.class,
        "{\"type\":\"record\",\"name\":\"RAvroNameCollide\",\"namespace\":"
            + "\"org.apache.avro.reflect.TestReflect\",\"fields\":[" + "{\"name\":\"b\",\"type\":\"int\"},"
            + "{\"name\":\"b\",\"type\":\"int\"}]}");
  }

  public static class RAvroStringableField {
    @Stringable
    int a;
  }

  @Test
  public void testAnnotationAvroStringableFields() throws Exception {
    check(RAvroStringableField.class, "{\"type\":\"record\",\"name\":\"RAvroStringableField\",\"namespace\":"
        + "\"org.apache.avro.reflect.TestReflect\",\"fields\":[" + "{\"name\":\"a\",\"type\":\"string\"}]}");
  }

  private void check(Object o, String schemaJson) {
    check(o.getClass(), schemaJson);
  }

  private void check(java.lang.reflect.Type type, String schemaJson) {
    assertEquals(schemaJson, ReflectData.get().getSchema(type).toString());
  }

  @Test
  public void testRecordIO() throws IOException {
    Schema schm = ReflectData.get().getSchema(SampleRecord.class);
    ReflectDatumWriter<SampleRecord> writer = new ReflectDatumWriter<>(schm);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    SampleRecord record = new SampleRecord();
    record.x = 5;
    record.y = 10;
    writer.write(record, factory.directBinaryEncoder(out, null));
    ReflectDatumReader<SampleRecord> reader = new ReflectDatumReader<>(schm);
    SampleRecord decoded = reader.read(null, DecoderFactory.get().binaryDecoder(out.toByteArray(), null));
    assertEquals(record, decoded);
  }

  public static class AvroEncRecord {
    @AvroEncode(using = DateAsLongEncoding.class)
    java.util.Date date;

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof AvroEncRecord))
        return false;
      return date.equals(((AvroEncRecord) o).date);
    }
  }

  public static class multipleAnnotationRecord {
    @AvroIgnore
    @Stringable
    Integer i1;

    @AvroIgnore
    @Nullable
    Integer i2;

    @AvroIgnore
    @AvroName("j")
    Integer i3;

    @AvroIgnore
    @AvroEncode(using = DateAsLongEncoding.class)
    java.util.Date i4;

    @Stringable
    @Nullable
    Integer i5;

    @Stringable
    @AvroName("j6")
    Integer i6 = 6;

    @Stringable
    @AvroEncode(using = DateAsLongEncoding.class)
    java.util.Date i7 = new java.util.Date(7L);

    @Nullable
    @AvroName("j8")
    Integer i8;

    @Nullable
    @AvroEncode(using = DateAsLongEncoding.class)
    java.util.Date i9;

    @AvroName("j10")
    @AvroEncode(using = DateAsLongEncoding.class)
    java.util.Date i10 = new java.util.Date(10L);

    @Stringable
    @Nullable
    @AvroName("j11")
    @AvroEncode(using = DateAsLongEncoding.class)
    java.util.Date i11;
  }

  @Test
  public void testMultipleAnnotations() throws IOException {
    Schema schm = ReflectData.get().getSchema(multipleAnnotationRecord.class);
    ReflectDatumWriter<multipleAnnotationRecord> writer = new ReflectDatumWriter<>(schm);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    multipleAnnotationRecord record = new multipleAnnotationRecord();
    record.i1 = 1;
    record.i2 = 2;
    record.i3 = 3;
    record.i4 = new java.util.Date(4L);
    record.i5 = 5;
    record.i6 = 6;
    record.i7 = new java.util.Date(7L);
    record.i8 = 8;
    record.i9 = new java.util.Date(9L);
    record.i10 = new java.util.Date(10L);
    record.i11 = new java.util.Date(11L);

    writer.write(record, factory.directBinaryEncoder(out, null));
    ReflectDatumReader<multipleAnnotationRecord> reader = new ReflectDatumReader<>(schm);
    multipleAnnotationRecord decoded = reader.read(new multipleAnnotationRecord(),
        DecoderFactory.get().binaryDecoder(out.toByteArray(), null));
    assertTrue(decoded.i1 == null);
    assertTrue(decoded.i2 == null);
    assertTrue(decoded.i3 == null);
    assertTrue(decoded.i4 == null);
    assertTrue(decoded.i5 == 5);
    assertTrue(decoded.i6 == 6);
    assertTrue(decoded.i7.getTime() == 7);
    assertTrue(decoded.i8 == 8);
    assertTrue(decoded.i9.getTime() == 9);
    assertTrue(decoded.i10.getTime() == 10);
    assertTrue(decoded.i11.getTime() == 11);
  }

  @Test
  public void testAvroEncodeInducing() throws IOException {
    Schema schm = ReflectData.get().getSchema(AvroEncRecord.class);
    assertEquals(schm.toString(),
        "{\"type\":\"record\",\"name\":\"AvroEncRecord\",\"namespace"
            + "\":\"org.apache.avro.reflect.TestReflect\",\"fields\":[{\"name\":\"date\","
            + "\"type\":{\"type\":\"long\",\"CustomEncoding\":\"DateAsLongEncoding\"}}]}");
  }

  @Test
  public void testAvroEncodeIO() throws IOException {
    Schema schm = ReflectData.get().getSchema(AvroEncRecord.class);
    ReflectDatumWriter<AvroEncRecord> writer = new ReflectDatumWriter<>(schm);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    AvroEncRecord record = new AvroEncRecord();
    record.date = new java.util.Date(948833323L);
    writer.write(record, factory.directBinaryEncoder(out, null));
    ReflectDatumReader<AvroEncRecord> reader = new ReflectDatumReader<>(schm);
    AvroEncRecord decoded = reader.read(new AvroEncRecord(),
        DecoderFactory.get().binaryDecoder(out.toByteArray(), null));
    assertEquals(record, decoded);
  }

  @Test
  public void testRecordWithNullIO() throws IOException {
    ReflectData reflectData = ReflectData.AllowNull.get();
    Schema schm = reflectData.getSchema(AnotherSampleRecord.class);
    ReflectDatumWriter<AnotherSampleRecord> writer = new ReflectDatumWriter<>(schm);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    // keep record.a null and see if that works
    Encoder e = factory.directBinaryEncoder(out, null);
    AnotherSampleRecord a = new AnotherSampleRecord();
    writer.write(a, e);
    AnotherSampleRecord b = new AnotherSampleRecord(10);
    writer.write(b, e);
    e.flush();
    ReflectDatumReader<AnotherSampleRecord> reader = new ReflectDatumReader<>(schm);
    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    Decoder d = DecoderFactory.get().binaryDecoder(in, null);
    AnotherSampleRecord decoded = reader.read(null, d);
    assertEquals(a, decoded);
    decoded = reader.read(null, d);
    assertEquals(b, decoded);
  }

  @Test
  public void testDisableUnsafe() throws Exception {
    String saved = System.getProperty("avro.disable.unsafe");
    try {
      System.setProperty("avro.disable.unsafe", "true");
      ReflectData.ACCESSOR_CACHE.remove(multipleAnnotationRecord.class);
      ReflectData.ACCESSOR_CACHE.remove(AnotherSampleRecord.class);
      ReflectionUtil.resetFieldAccess();
      testMultipleAnnotations();
      testRecordWithNullIO();
    } finally {
      if (saved == null)
        System.clearProperty("avro.disable.unsafe");
      else
        System.setProperty("avro.disable.unsafe", saved);
      ReflectData.ACCESSOR_CACHE.remove(multipleAnnotationRecord.class);
      ReflectData.ACCESSOR_CACHE.remove(AnotherSampleRecord.class);
      ReflectionUtil.resetFieldAccess();
    }
  }

  public static class SampleRecord {
    public int x = 1;
    private int y = 2;

    @Override
    public int hashCode() {
      return x + y;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      final SampleRecord other = (SampleRecord) obj;
      if (x != other.x)
        return false;
      return y == other.y;
    }

    public static class AnotherSampleRecord {
      private Integer a = null;
      private SampleRecord s = null;

      public AnotherSampleRecord() {
      }

      AnotherSampleRecord(Integer a) {
        this.a = a;
        this.s = new SampleRecord();
      }

      @Override
      public int hashCode() {
        int hash = (a != null ? a.hashCode() : 0);
        hash += (s != null ? s.hashCode() : 0);
        return hash;
      }

      @Override
      public boolean equals(Object other) {
        if (other instanceof AnotherSampleRecord) {
          AnotherSampleRecord o = (AnotherSampleRecord) other;
          return (this.a != null || o.a == null) && (this.a == null || this.a.equals(o.a))
              && (this.s != null || o.s == null) && (this.s == null || this.s.equals(o.s));
        } else {
          return false;
        }
      }
    }
  }

  public static class X {
    int i;
  }

  public static class B1 {
    X x;
  }

  public static class B2 {
    X x;
  }

  public static class A {
    B1 b1;
    B2 b2;
  }

  public static interface C {
    void foo(A a);
  }

  @Test
  public void testForwardReference() {
    ReflectData data = ReflectData.get();
    Protocol reflected = data.getProtocol(C.class);
    Protocol reparsed = Protocol.parse(reflected.toString());
    assertEquals(reflected, reparsed);
    assert (reparsed.getTypes().contains(data.getSchema(A.class)));
    assert (reparsed.getTypes().contains(data.getSchema(B1.class)));
    assert (reparsed.getTypes().contains(data.getSchema(B2.class)));
    assert (reparsed.getTypes().contains(data.getSchema(X.class)));
  }

  public static interface P3 {
    void m1();

    void m1(int x);
  }

  @Test(expected = AvroTypeException.class)
  public void testOverloadedMethod() {
    ReflectData.get().getProtocol(P3.class);
  }

  @Test
  public void testNoPackageSchema() throws Exception {
    ReflectData.get().getSchema(Class.forName("NoPackage"));
  }

  @Test
  public void testNoPackageProtocol() throws Exception {
    ReflectData.get().getProtocol(Class.forName("NoPackage"));
  }

  public static class Y {
    int i;
  }

  @Test
  /** Test nesting of reflect data within generic. */
  public void testReflectWithinGeneric() throws Exception {
    ReflectData data = ReflectData.get();
    // define a record with a field that's a specific Y
    Schema schema = Schema.createRecord("Foo", "", "x.y.z", false);
    List<Schema.Field> fields = new ArrayList<>();
    fields.add(new Schema.Field("f", data.getSchema(Y.class), "", null));
    schema.setFields(fields);

    // create a generic instance of this record
    Y y = new Y();
    y.i = 1;
    GenericData.Record record = new GenericData.Record(schema);
    record.put("f", y);

    // test that this instance can be written & re-read
    checkBinary(schema, record);
  }

  @Test
  public void testPrimitiveArray() throws Exception {
    testPrimitiveArrays(false);
  }

  @Test
  public void testPrimitiveArrayBlocking() throws Exception {
    testPrimitiveArrays(true);
  }

  private void testPrimitiveArrays(boolean blocking) throws Exception {
    testPrimitiveArray(boolean.class, blocking);
    testPrimitiveArray(byte.class, blocking);
    testPrimitiveArray(short.class, blocking);
    testPrimitiveArray(char.class, blocking);
    testPrimitiveArray(int.class, blocking);
    testPrimitiveArray(long.class, blocking);
    testPrimitiveArray(float.class, blocking);
    testPrimitiveArray(double.class, blocking);
  }

  private void testPrimitiveArray(Class<?> c, boolean blocking) throws Exception {
    ReflectData data = new ReflectData();
    Random r = new Random();
    int size = 200;
    Object array = Array.newInstance(c, size);
    Schema s = data.getSchema(array.getClass());
    for (int i = 0; i < size; i++) {
      Array.set(array, i, randomFor(c, r));
    }
    checkBinary(data, s, array, false, blocking);
  }

  private Object randomFor(Class<?> c, Random r) {
    if (c == boolean.class)
      return r.nextBoolean();
    if (c == int.class)
      return r.nextInt();
    if (c == long.class)
      return r.nextLong();
    if (c == byte.class)
      return (byte) r.nextInt();
    if (c == float.class)
      return r.nextFloat();
    if (c == double.class)
      return r.nextDouble();
    if (c == char.class)
      return (char) r.nextInt();
    if (c == short.class)
      return (short) r.nextInt();
    return null;
  }

  /** Test union of null and an array. */
  @Test
  public void testNullArray() throws Exception {
    String json = "[{\"type\":\"array\", \"items\": \"long\"}, \"null\"]";
    Schema schema = new Schema.Parser().parse(json);
    checkBinary(schema, null);
  }

  /** Test stringable classes. */
  @Test
  public void testStringables() throws Exception {
    checkStringable(java.math.BigDecimal.class, "10");
    checkStringable(java.math.BigInteger.class, "20");
    checkStringable(java.net.URI.class, "foo://bar:9000/baz");
    checkStringable(java.net.URL.class, "http://bar:9000/baz");
    checkStringable(java.io.File.class, "foo.bar");
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void checkStringable(Class c, String value) throws Exception {
    ReflectData data = new ReflectData();
    Schema schema = data.getSchema(c);
    assertEquals("{\"type\":\"string\",\"java-class\":\"" + c.getName() + "\"}", schema.toString());
    checkBinary(schema, c.getConstructor(String.class).newInstance(value));
  }

  public static class M1 {
    Map<Integer, String> integerKeyMap;
    Map<java.math.BigInteger, String> bigIntegerKeyMap;
    Map<java.math.BigDecimal, String> bigDecimalKeyMap;
    Map<java.io.File, String> fileKeyMap;
  }

  /** Test Map with stringable key classes. */
  @Test
  public void testStringableMapKeys() throws Exception {
    M1 record = new M1();
    record.integerKeyMap = new HashMap<>(1);
    record.integerKeyMap.put(10, "foo");

    record.bigIntegerKeyMap = new HashMap<>(1);
    record.bigIntegerKeyMap.put(java.math.BigInteger.TEN, "bar");

    record.bigDecimalKeyMap = new HashMap<>(1);
    record.bigDecimalKeyMap.put(java.math.BigDecimal.ONE, "bigDecimal");

    record.fileKeyMap = new HashMap<>(1);
    record.fileKeyMap.put(new java.io.File("foo.bar"), "file");

    ReflectData data = new ReflectData().addStringable(Integer.class);

    checkBinary(data, data.getSchema(M1.class), record, true);
  }

  public static class NullableStringable {
    java.math.BigDecimal number;
  }

  @Test
  public void testNullableStringableField() throws Exception {
    NullableStringable datum = new NullableStringable();
    datum.number = java.math.BigDecimal.TEN;

    Schema schema = ReflectData.AllowNull.get().getSchema(NullableStringable.class);
    checkBinary(schema, datum);
  }

  public static void checkBinary(ReflectData reflectData, Schema schema, Object datum, boolean equals)
      throws IOException {
    checkBinary(reflectData, schema, datum, equals, false);
  }

  private static void checkBinary(ReflectData reflectData, Schema schema, Object datum, boolean equals,
      boolean blocking) throws IOException {
    ReflectDatumWriter<Object> writer = new ReflectDatumWriter<>(schema);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    if (!blocking) {
      writer.write(datum, EncoderFactory.get().directBinaryEncoder(out, null));
    } else {
      writer.write(datum, new EncoderFactory().configureBlockSize(64).blockingBinaryEncoder(out, null));
    }
    writer.write(datum, EncoderFactory.get().directBinaryEncoder(out, null));
    byte[] data = out.toByteArray();

    ReflectDatumReader<Object> reader = new ReflectDatumReader<>(schema);
    Object decoded = reader.read(null, DecoderFactory.get().binaryDecoder(data, null));

    assertEquals(0, reflectData.compare(datum, decoded, schema, equals));
  }

  public static void checkBinary(Schema schema, Object datum) throws IOException {
    checkBinary(ReflectData.get(), schema, datum, false);
  }

  /** Test that the error message contains the name of the class. */
  @Test
  public void testReflectFieldError() throws Exception {
    Object datum = "";
    try {
      ReflectData.get().getField(datum, "notAFieldOfString", 0);
    } catch (AvroRuntimeException e) {
      assertTrue(e.getMessage().contains(datum.getClass().getName()));
    }
  }

  @AvroAlias(alias = "a", space = "b")
  private static class AliasA {
  }

  @AvroAlias(alias = "a", space = "")
  private static class AliasB {
  }

  @AvroAlias(alias = "a")
  private static class AliasC {
  }

  @Test
  public void testAvroAliasOnClass() {
    check(AliasA.class,
        "{\"type\":\"record\",\"name\":\"AliasA\",\"namespace\":\"org.apache.avro.reflect.TestReflect\",\"fields\":[],\"aliases\":[\"b.a\"]}");
    check(AliasB.class,
        "{\"type\":\"record\",\"name\":\"AliasB\",\"namespace\":\"org.apache.avro.reflect.TestReflect\",\"fields\":[],\"aliases\":[\"a\"]}");
    check(AliasC.class,
        "{\"type\":\"record\",\"name\":\"AliasC\",\"namespace\":\"org.apache.avro.reflect.TestReflect\",\"fields\":[],\"aliases\":[\"a\"]}");
  }

  @AvroAlias(alias = "alias1", space = "space1")
  @AvroAlias(alias = "alias2", space = "space2")
  private static class MultipleAliasRecord {

  }

  @Test
  public void testMultipleAliasAnnotationsOnClass() {
    check(MultipleAliasRecord.class,
        "{\"type\":\"record\",\"name\":\"MultipleAliasRecord\",\"namespace\":\"org.apache.avro.reflect.TestReflect\",\"fields\":[],\"aliases\":[\"space1.alias1\",\"space2.alias2\"]}");

  }

  private static class Z {
  }

  @Test
  public void testDollarTerminatedNamespaceCompatibility() {
    ReflectData data = ReflectData.get();
    Schema s = new Schema.Parser().setValidate(false).parse(
        "{\"type\":\"record\",\"name\":\"Z\",\"namespace\":\"org.apache.avro.reflect.TestReflect$\",\"fields\":[]}");
    assertEquals(data.getSchema(data.getClass(s)).toString(),
        "{\"type\":\"record\",\"name\":\"Z\",\"namespace\":\"org.apache.avro.reflect.TestReflect\",\"fields\":[]}");
  }

  @Test
  public void testDollarTerminatedNestedStaticClassNamespaceCompatibility() {
    ReflectData data = ReflectData.get();
    // Older versions of Avro generated this namespace on nested records.
    Schema s = new Schema.Parser().setValidate(false).parse(
        "{\"type\":\"record\",\"name\":\"AnotherSampleRecord\",\"namespace\":\"org.apache.avro.reflect.TestReflect$SampleRecord\",\"fields\":[]}");
    assertThat(data.getSchema(data.getClass(s)).getFullName(),
        is("org.apache.avro.reflect.TestReflect.SampleRecord.AnotherSampleRecord"));
  }

  private static class ClassWithAliasOnField {
    @AvroAlias(alias = "aliasName")
    int primitiveField;
  }

  private static class ClassWithMultipleAliasesOnField {
    @AvroAlias(alias = "alias1")
    @AvroAlias(alias = "alias2")
    int primitiveField;
  }

  private static class ClassWithAliasAndNamespaceOnField {
    @AvroAlias(alias = "aliasName", space = "forbidden.space.entry")
    int primitiveField;
  }

  @Test
  public void testAvroAliasOnField() {

    Schema expectedSchema = SchemaBuilder.record(ClassWithAliasOnField.class.getSimpleName())
        .namespace("org.apache.avro.reflect.TestReflect").fields().name("primitiveField").aliases("aliasName")
        .type(Schema.create(org.apache.avro.Schema.Type.INT)).noDefault().endRecord();

    check(ClassWithAliasOnField.class, expectedSchema.toString());
  }

  @Test(expected = AvroRuntimeException.class)
  public void namespaceDefinitionOnFieldAliasMustThrowException() {
    ReflectData.get().getSchema(ClassWithAliasAndNamespaceOnField.class);
  }

  @Test
  public void testMultipleFieldAliases() {

    Schema expectedSchema = SchemaBuilder.record(ClassWithMultipleAliasesOnField.class.getSimpleName())
        .namespace("org.apache.avro.reflect.TestReflect").fields().name("primitiveField").aliases("alias1", "alias2")
        .type(Schema.create(org.apache.avro.Schema.Type.INT)).noDefault().endRecord();

    check(ClassWithMultipleAliasesOnField.class, expectedSchema.toString());
  }

  private static class DefaultTest {
    @AvroDefault("1")
    int foo;
  }

  @Test
  public void testAvroDefault() {
    check(DefaultTest.class,
        "{\"type\":\"record\",\"name\":\"DefaultTest\","
            + "\"namespace\":\"org.apache.avro.reflect.TestReflect\",\"fields\":["
            + "{\"name\":\"foo\",\"type\":\"int\",\"default\":1}]}");
  }

  public static class NullableBytesTest {
    @Nullable
    byte[] bytes;

    NullableBytesTest() {
    }

    NullableBytesTest(byte[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof NullableBytesTest && Arrays.equals(((NullableBytesTest) obj).bytes, this.bytes);
    }
  }

  @Test
  public void testNullableByteArrayNotNullValue() throws Exception {
    checkReadWrite(new NullableBytesTest("foo".getBytes(UTF_8)));
  }

  @Test
  public void testNullableByteArrayNullValue() throws Exception {
    checkReadWrite(new NullableBytesTest());
  }

  private enum DocTestEnum {
    ENUM_1, ENUM_2
  }

  @AvroDoc("DocTest class docs")
  private static class DocTest {
    @AvroDoc("Some Documentation")
    int foo;

    @AvroDoc("Some other Documentation")
    DocTestEnum enums;

    @AvroDoc("And again")
    DefaultTest defaultTest;
  }

  @Test
  public void testAvroDoc() {
    check(DocTest.class,
        "{\"type\":\"record\",\"name\":\"DocTest\",\"namespace\":\"org.apache.avro.reflect.TestReflect\","
            + "\"doc\":\"DocTest class docs\"," + "\"fields\":["
            + "{\"name\":\"defaultTest\",\"type\":{\"type\":\"record\",\"name\":\"DefaultTest\","
            + "\"fields\":[{\"name\":\"foo\",\"type\":\"int\",\"default\":1}]},\"doc\":\"And again\"},"
            + "{\"name\":\"enums\",\"type\":{\"type\":\"enum\",\"name\":\"DocTestEnum\","
            + "\"symbols\":[\"ENUM_1\",\"ENUM_2\"]},\"doc\":\"Some other Documentation\"},"
            + "{\"name\":\"foo\",\"type\":\"int\",\"doc\":\"Some Documentation\"}" + "]}");
  }

}
