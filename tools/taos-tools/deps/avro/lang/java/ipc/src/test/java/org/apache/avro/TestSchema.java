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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.compiler.specific.TestSpecificCompiler;
import org.apache.avro.data.Json;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.RandomData;
import org.apache.avro.util.Utf8;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import static org.junit.Assert.*;

public class TestSchema {

  @Rule
  public TestName name = new TestName();

  @Rule
  public TemporaryFolder DIR = new TemporaryFolder();

  public static final String LISP_SCHEMA = "{\"type\": \"record\", \"name\": \"Lisp\", \"fields\": ["
      + "{\"name\":\"value\", \"type\":[\"null\", \"string\","
      + "{\"type\": \"record\", \"name\": \"Cons\", \"fields\": [" + "{\"name\":\"car\", \"type\":\"Lisp\"},"
      + "{\"name\":\"cdr\", \"type\":\"Lisp\"}]}]}]}";

  public static final String BASIC_ENUM_SCHEMA = "{\"type\":\"enum\", \"name\":\"Test\","
      + "\"symbols\": [\"A\", \"B\"]}";

  public static final String SCHEMA_WITH_DOC_TAGS = "{\n" + "  \"type\": \"record\",\n"
      + "  \"name\": \"outer_record\",\n" + "  \"doc\": \"This is not a world record.\",\n" + "  \"fields\": [\n"
      + "    { \"type\": { \"type\": \"fixed\", \"doc\": \"Very Inner Fixed\", "
      + "                  \"name\": \"very_inner_fixed\", \"size\": 1 },\n"
      + "      \"doc\": \"Inner Fixed\", \"name\": \"inner_fixed\" },\n" + "    { \"type\": \"string\",\n"
      + "      \"name\": \"inner_string\",\n" + "      \"doc\": \"Inner String\" },\n"
      + "    { \"type\": { \"type\": \"enum\", \"doc\": \"Very Inner Enum\", \n"
      + "                  \"name\": \"very_inner_enum\", \n"
      + "                  \"symbols\": [ \"A\", \"B\", \"C\" ] },\n"
      + "      \"doc\": \"Inner Enum\", \"name\": \"inner_enum\" },\n"
      + "    { \"type\": [\"string\", \"int\"], \"doc\": \"Inner Union\", \n" + "      \"name\": \"inner_union\" }\n"
      + "  ]\n" + "}\n";

  private static final int COUNT = Integer.parseInt(System.getProperty("test.count", "30"));

  @Test
  public void testNull() throws Exception {
    assertEquals(Schema.create(Type.NULL), new Schema.Parser().parse("\"null\""));
    assertEquals(Schema.create(Type.NULL), new Schema.Parser().parse("{\"type\":\"null\"}"));
    check(new File(DIR.getRoot(), name.getMethodName()), "\"null\"", "null", null);
  }

  @Test
  public void testBoolean() throws Exception {
    assertEquals(Schema.create(Type.BOOLEAN), new Schema.Parser().parse("\"boolean\""));
    assertEquals(Schema.create(Type.BOOLEAN), new Schema.Parser().parse("{\"type\":\"boolean\"}"));
    check(new File(DIR.getRoot(), name.getMethodName()), "\"boolean\"", "true", Boolean.TRUE);
  }

  @Test
  public void testString() throws Exception {
    assertEquals(Schema.create(Type.STRING), new Schema.Parser().parse("\"string\""));
    assertEquals(Schema.create(Type.STRING), new Schema.Parser().parse("{\"type\":\"string\"}"));
    check(new File(DIR.getRoot(), name.getMethodName()), "\"string\"", "\"foo\"", new Utf8("foo"));
  }

  @Test
  public void testBytes() throws Exception {
    assertEquals(Schema.create(Type.BYTES), new Schema.Parser().parse("\"bytes\""));
    assertEquals(Schema.create(Type.BYTES), new Schema.Parser().parse("{\"type\":\"bytes\"}"));
    check(new File(DIR.getRoot(), name.getMethodName()), "\"bytes\"", "\"\\u0000ABC\\u00FF\"",
        ByteBuffer.wrap(new byte[] { 0, 65, 66, 67, -1 }));
  }

  @Test
  public void testInt() throws Exception {
    assertEquals(Schema.create(Type.INT), new Schema.Parser().parse("\"int\""));
    assertEquals(Schema.create(Type.INT), new Schema.Parser().parse("{\"type\":\"int\"}"));
    check(new File(DIR.getRoot(), name.getMethodName()), "\"int\"", "9", 9);
  }

  @Test
  public void testLong() throws Exception {
    assertEquals(Schema.create(Type.LONG), new Schema.Parser().parse("\"long\""));
    assertEquals(Schema.create(Type.LONG), new Schema.Parser().parse("{\"type\":\"long\"}"));
    check(new File(DIR.getRoot(), name.getMethodName()), "\"long\"", "11", 11L);
  }

  @Test
  public void testFloat() throws Exception {
    assertEquals(Schema.create(Type.FLOAT), new Schema.Parser().parse("\"float\""));
    assertEquals(Schema.create(Type.FLOAT), new Schema.Parser().parse("{\"type\":\"float\"}"));
    check(new File(DIR.getRoot(), name.getMethodName()), "\"float\"", "1.1", 1.1f);
    checkDefault("\"float\"", "\"NaN\"", Float.NaN);
    checkDefault("\"float\"", "\"Infinity\"", Float.POSITIVE_INFINITY);
    checkDefault("\"float\"", "\"-Infinity\"", Float.NEGATIVE_INFINITY);
  }

  @Test
  public void testDouble() throws Exception {
    assertEquals(Schema.create(Type.DOUBLE), new Schema.Parser().parse("\"double\""));
    assertEquals(Schema.create(Type.DOUBLE), new Schema.Parser().parse("{\"type\":\"double\"}"));
    check(new File(DIR.getRoot(), name.getMethodName()), "\"double\"", "1.2", 1.2);
    checkDefault("\"double\"", "\"NaN\"", Double.NaN);
    checkDefault("\"double\"", "\"Infinity\"", Double.POSITIVE_INFINITY);
    checkDefault("\"double\"", "\"-Infinity\"", Double.NEGATIVE_INFINITY);
  }

  @Test
  public void testArray() throws Exception {
    String json = "{\"type\":\"array\", \"items\": \"long\"}";
    Schema schema = new Schema.Parser().parse(json);
    Collection<Long> array = new GenericData.Array<>(1, schema);
    array.add(1L);
    check(new File(DIR.getRoot(), name.getMethodName()), json, "[1]", array);
    array = new ArrayList<>(1);
    array.add(1L);
    check(new File(DIR.getRoot(), name.getMethodName()), json, "[1]", array);
    checkParseError("{\"type\":\"array\"}"); // items required
  }

  @Test
  public void testMap() throws Exception {
    HashMap<Utf8, Long> map = new HashMap<>();
    map.put(new Utf8("a"), 1L);
    check(new File(DIR.getRoot(), name.getMethodName()), "{\"type\":\"map\", \"values\":\"long\"}", "{\"a\":1}", map);
    checkParseError("{\"type\":\"map\"}"); // values required
  }

  @Test
  public void testUnionMap() throws Exception {
    String unionMapSchema = "{\"name\":\"foo\", \"type\":\"record\"," + " \"fields\":[ {\"name\":\"mymap\", \"type\":"
        + "   [{\"type\":\"map\", \"values\":" + "      [\"int\",\"long\",\"float\",\"string\"]}," + "    \"null\"]"
        + "   }]" + " }";
    check(new File(DIR.getRoot(), name.getMethodName()), unionMapSchema, true);
  }

  @Test
  public void testRecord() throws Exception {
    String recordJson = "{\"type\":\"record\", \"name\":\"Test\", \"fields\":"
        + "[{\"name\":\"f\", \"type\":\"long\", \"foo\":\"bar\"}]}";
    Schema schema = new Schema.Parser().parse(recordJson);

    GenericData.Record record = new GenericData.Record(schema);
    record.put("f", 11L);
    check(new File(DIR.getRoot(), name.getMethodName()), recordJson, "{\"f\":11}", record, false);

    // test field props
    assertEquals("bar", schema.getField("f").getProp("foo"));
    assertEquals("bar", new Schema.Parser().parse(schema.toString()).getField("f").getProp("foo"));
    schema.getField("f").addProp("baz", "boo");
    assertEquals("boo", schema.getField("f").getProp("baz"));

    checkParseError("{\"type\":\"record\"}");
    checkParseError("{\"type\":\"record\",\"name\":\"X\"}");
    checkParseError("{\"type\":\"record\",\"name\":\"X\",\"fields\":\"Y\"}");
    checkParseError("{\"type\":\"record\",\"name\":\"X\",\"fields\":" + "[{\"name\":\"f\"}]}"); // no type
    checkParseError("{\"type\":\"record\",\"name\":\"X\",\"fields\":" + "[{\"type\":\"long\"}]}"); // no name
    // check invalid record names
    checkParseError("{\"type\":\"record\",\"name\":\"1X\",\"fields\":[]}");
    checkParseError("{\"type\":\"record\",\"name\":\"X$\",\"fields\":[]}");
    // check invalid field names
    checkParseError("{\"type\":\"record\",\"name\":\"X\",\"fields\":[" + "{\"name\":\"1f\",\"type\":\"int\"}]}");
    checkParseError("{\"type\":\"record\",\"name\":\"X\",\"fields\":[" + "{\"name\":\"f$\",\"type\":\"int\"}]}");
    checkParseError("{\"type\":\"record\",\"name\":\"X\",\"fields\":[" + "{\"name\":\"f.g\",\"type\":\"int\"}]}");
  }

  @Test
  public void testInvalidNameTolerance() {
    new Schema.Parser().setValidate(false).parse("{\"type\":\"record\",\"name\":\"1X\",\"fields\":[]}");
    new Schema.Parser().setValidate(false).parse("{\"type\":\"record\",\"name\":\"X-\",\"fields\":[]}");
    new Schema.Parser().setValidate(false).parse("{\"type\":\"record\",\"name\":\"X$\",\"fields\":[]}");
  }

  @Test
  public void testMapInRecord() throws Exception {
    String json = "{\"type\":\"record\", \"name\":\"Test\", \"fields\":"
        + "[{\"name\":\"f\", \"type\": {\"type\":\"map\", \"values\":\"long\"}}]}";
    Schema schema = new Schema.Parser().parse(json);

    HashMap<Utf8, Long> map = new HashMap<>();
    map.put(new Utf8("a"), 1L);
    GenericData.Record record = new GenericData.Record(schema);
    record.put("f", map);
    check(new File(DIR.getRoot(), name.getMethodName()), json, "{\"f\":{\"a\":1}}", record, false);
  }

  @Test
  public void testEnum() throws Exception {
    check(new File(DIR.getRoot(), name.getMethodName()), BASIC_ENUM_SCHEMA, "\"B\"",
        new GenericData.EnumSymbol(new Schema.Parser().parse(BASIC_ENUM_SCHEMA), "B"), false);
    checkParseError("{\"type\":\"enum\"}"); // symbols required
    checkParseError("{\"type\":\"enum\",\"symbols\": [\"X\"]}"); // name reqd
    // check no duplicate symbols
    checkParseError("{\"type\":\"enum\",\"name\":\"X\",\"symbols\":[\"X\",\"X\"]}");
    // check no invalid symbols
    checkParseError("{\"type\":\"enum\",\"name\":\"X\",\"symbols\":[\"1X\"]}");
    checkParseError("{\"type\":\"enum\",\"name\":\"X\",\"symbols\":[\"X$\"]}");
    checkParseError("{\"type\":\"enum\",\"name\":\"X\",\"symbols\":[\"X.Y\"]}");
  }

  @Test
  public void testFixed() throws Exception {
    String json = "{\"type\": \"fixed\", \"name\":\"Test\", \"size\": 1}";
    Schema schema = new Schema.Parser().parse(json);
    check(new File(DIR.getRoot(), name.getMethodName()), json, "\"a\"",
        new GenericData.Fixed(schema, new byte[] { (byte) 'a' }), false);
    checkParseError("{\"type\":\"fixed\"}"); // size required
  }

  @Test
  public void testRecursive() throws Exception {
    check(new File(DIR.getRoot(), name.getMethodName()),
        "{\"type\": \"record\", \"name\": \"Node\", \"fields\": [" + "{\"name\":\"label\", \"type\":\"string\"},"
            + "{\"name\":\"children\", \"type\":" + "{\"type\": \"array\", \"items\": \"Node\" }}]}",
        false);
  }

  @Test
  public void testRecursiveEquals() throws Exception {
    String jsonSchema = "{\"type\":\"record\", \"name\":\"List\", \"fields\": ["
        + "{\"name\":\"next\", \"type\":\"List\"}]}";
    Schema s1 = new Schema.Parser().parse(jsonSchema);
    Schema s2 = new Schema.Parser().parse(jsonSchema);
    assertEquals(s1, s2);
    s1.hashCode(); // test no stackoverflow
  }

  @Test
  /**
   * Test that equals() and hashCode() don't require exponential time on certain
   * pathological schemas.
   */
  public void testSchemaExplosion() throws Exception {
    for (int i = 1; i < 15; i++) { // 15 is big enough to trigger
      // create a list of records, each with a single field whose type is a
      // union of all of the records.
      List<Schema> recs = new ArrayList<>();
      for (int j = 0; j < i; j++)
        recs.add(Schema.createRecord("" + (char) ('A' + j), null, null, false));
      for (Schema s : recs) {
        Schema union = Schema.createUnion(recs);
        Field f = new Field("x", union, null, null);
        List<Field> fields = new ArrayList<>();
        fields.add(f);
        s.setFields(fields);
      }
      // check that equals and hashcode are correct and complete in a
      // reasonable amount of time
      for (Schema s1 : recs) {
        Schema s2 = new Schema.Parser().parse(s1.toString());
        assertEquals(s1.hashCode(), s2.hashCode());
        assertEquals(s1, s2);
      }
    }
  }

  @Test
  public void testLisp() throws Exception {
    check(new File(DIR.getRoot(), name.getMethodName()), LISP_SCHEMA, false);
  }

  @Test
  public void testUnion() throws Exception {
    check(new File(DIR.getRoot(), name.getMethodName()), "[\"string\", \"long\"]", false);
    checkDefault("[\"double\", \"long\"]", "1.1", 1.1);

    // test that erroneous default values cause errors
    for (String type : new String[] { "int", "long", "float", "double", "string", "bytes", "boolean" }) {
      checkValidateDefaults("[\"" + type + "\", \"null\"]", "null"); // schema parse time
      boolean error = false;
      try {
        checkDefault("[\"" + type + "\", \"null\"]", "null", 0); // read time
      } catch (AvroTypeException e) {
        error = true;
      }
      assertTrue(error);
      checkValidateDefaults("[\"null\", \"" + type + "\"]", "0"); // schema parse time
      error = false;
      try {
        checkDefault("[\"null\", \"" + type + "\"]", "0", null); // read time
      } catch (AvroTypeException e) {
        error = true;
      }
      assertTrue(error);
    }

    // check union json
    String record = "{\"type\":\"record\",\"name\":\"Foo\",\"fields\":[]}";
    String fixed = "{\"type\":\"fixed\",\"name\":\"Bar\",\"size\": 1}";
    String enu = "{\"type\":\"enum\",\"name\":\"Baz\",\"symbols\": [\"X\"]}";
    Schema union = new Schema.Parser().parse("[\"null\",\"string\"," + record + "," + enu + "," + fixed + "]");
    checkJson(union, null, "null");
    checkJson(union, new Utf8("foo"), "{\"string\":\"foo\"}");
    checkJson(union, new GenericData.Record(new Schema.Parser().parse(record)), "{\"Foo\":{}}");
    checkJson(union, new GenericData.Fixed(new Schema.Parser().parse(fixed), new byte[] { (byte) 'a' }),
        "{\"Bar\":\"a\"}");
    checkJson(union, new GenericData.EnumSymbol(new Schema.Parser().parse(enu), "X"), "{\"Baz\":\"X\"}");
  }

  @Test
  public void testComplexUnions() throws Exception {
    // one of each unnamed type and two of named types
    String partial = "[\"int\", \"long\", \"float\", \"double\", \"boolean\", \"bytes\","
        + " \"string\", {\"type\":\"array\", \"items\": \"long\"}," + " {\"type\":\"map\", \"values\":\"long\"}";
    String namedTypes = ", {\"type\":\"record\",\"name\":\"Foo\",\"fields\":[]},"
        + " {\"type\":\"fixed\",\"name\":\"Bar\",\"size\": 1},"
        + " {\"type\":\"enum\",\"name\":\"Baz\",\"symbols\": [\"X\"]}";

    String namedTypes2 = ", {\"type\":\"record\",\"name\":\"Foo2\",\"fields\":[]},"
        + " {\"type\":\"fixed\",\"name\":\"Bar2\",\"size\": 1},"
        + " {\"type\":\"enum\",\"name\":\"Baz2\",\"symbols\": [\"X\"]}";

    check(new File(DIR.getRoot(), name.getMethodName()), partial + namedTypes + "]", false);
    check(new File(DIR.getRoot(), name.getMethodName()), partial + namedTypes + namedTypes2 + "]", false);
    checkParseError(partial + namedTypes + namedTypes + "]");

    // fail with two branches of the same unnamed type
    checkUnionError(new Schema[] { Schema.create(Type.INT), Schema.create(Type.INT) });
    checkUnionError(new Schema[] { Schema.create(Type.LONG), Schema.create(Type.LONG) });
    checkUnionError(new Schema[] { Schema.create(Type.FLOAT), Schema.create(Type.FLOAT) });
    checkUnionError(new Schema[] { Schema.create(Type.DOUBLE), Schema.create(Type.DOUBLE) });
    checkUnionError(new Schema[] { Schema.create(Type.BOOLEAN), Schema.create(Type.BOOLEAN) });
    checkUnionError(new Schema[] { Schema.create(Type.BYTES), Schema.create(Type.BYTES) });
    checkUnionError(new Schema[] { Schema.create(Type.STRING), Schema.create(Type.STRING) });
    checkUnionError(
        new Schema[] { Schema.createArray(Schema.create(Type.INT)), Schema.createArray(Schema.create(Type.INT)) });
    checkUnionError(
        new Schema[] { Schema.createMap(Schema.create(Type.INT)), Schema.createMap(Schema.create(Type.INT)) });

    List<String> symbols = new ArrayList<>();
    symbols.add("NOTHING");

    // succeed with two branches of the same named type, if different names
    Schema u;
    u = buildUnion(new Schema[] { new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"x.A\",\"fields\":[]}"),
        new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"y.A\",\"fields\":[]}") });
    check(new File(DIR.getRoot(), name.getMethodName()), u.toString(), false);

    u = buildUnion(new Schema[] { new Schema.Parser().parse("{\"type\":\"enum\",\"name\":\"x.A\",\"symbols\":[\"X\"]}"),
        new Schema.Parser().parse("{\"type\":\"enum\",\"name\":\"y.A\",\"symbols\":[\"Y\"]}") });
    check(new File(DIR.getRoot(), name.getMethodName()), u.toString(), false);

    u = buildUnion(new Schema[] { new Schema.Parser().parse("{\"type\":\"fixed\",\"name\":\"x.A\",\"size\":4}"),
        new Schema.Parser().parse("{\"type\":\"fixed\",\"name\":\"y.A\",\"size\":8}") });
    check(new File(DIR.getRoot(), name.getMethodName()), u.toString(), false);

    // fail with two branches of the same named type, but same names
    checkUnionError(new Schema[] { Schema.createRecord("Foo", null, "org.test", false),
        Schema.createRecord("Foo", null, "org.test", false) });
    checkUnionError(new Schema[] { Schema.createEnum("Bar", null, "org.test", symbols),
        Schema.createEnum("Bar", null, "org.test", symbols) });
    checkUnionError(new Schema[] { Schema.createFixed("Baz", null, "org.test", 2),
        Schema.createFixed("Baz", null, "org.test", 1) });

    Schema union = buildUnion(new Schema[] { Schema.create(Type.INT) });
    // fail if creating a union of a union
    checkUnionError(new Schema[] { union });
  }

  @Test
  public void testComplexProp() {
    String json = "{\"type\":\"null\", \"foo\": [0]}";
    Schema s = new Schema.Parser().parse(json);
    assertNull(s.getProp("foo"));
  }

  @Test
  public void testPropOrdering() {
    String json = "{\"type\":\"int\",\"z\":\"c\",\"yy\":\"b\",\"x\":\"a\"}";
    Schema s = new Schema.Parser().parse(json);
    assertEquals(json, s.toString());
  }

  @Test
  public void testParseInputStream() throws IOException {
    Schema s = new Schema.Parser().parse(new ByteArrayInputStream("\"boolean\"".getBytes(StandardCharsets.UTF_8)));
    assertEquals(new Schema.Parser().parse("\"boolean\""), s);
  }

  @Test
  public void testNamespaceScope() {
    String z = "{\"type\":\"record\",\"name\":\"Z\",\"fields\":[]}";
    String y = "{\"type\":\"record\",\"name\":\"q.Y\",\"fields\":[" + "{\"name\":\"f\",\"type\":" + z + "}]}";
    String x = "{\"type\":\"record\",\"name\":\"p.X\",\"fields\":[" + "{\"name\":\"f\",\"type\":" + y + "},"
        + "{\"name\":\"g\",\"type\":" + z + "}" + "]}";
    Schema xs = new Schema.Parser().parse(x);
    Schema ys = xs.getField("f").schema();
    assertEquals("p.Z", xs.getField("g").schema().getFullName());
    assertEquals("q.Z", ys.getField("f").schema().getFullName());
  }

  @Test
  public void testNamespaceNesting() {
    String y = "{\"type\":\"record\",\"name\":\"y.Y\",\"fields\":[" + "{\"name\":\"f\",\"type\":\"x.X\"}]}";
    String x = "{\"type\":\"record\",\"name\":\"x.X\",\"fields\":[" + "{\"name\":\"f\",\"type\":" + y + "}" + "]}";
    Schema xs = new Schema.Parser().parse(x);
    assertEquals(xs, new Schema.Parser().parse(xs.toString()));
  }

  @Test
  public void testNestedNullNamespace() {
    Schema inner = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Inner\",\"fields\":[]}");
    Schema outer = Schema.createRecord("Outer", null, "space", false);
    outer.setFields(Collections.singletonList(new Field("f", inner, null, null)));
    assertEquals(outer, new Schema.Parser().parse(outer.toString()));
  }

  @Test
  public void testDeeplyNestedNullNamespace() {
    Schema inner = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Inner\",\"fields\":["
        + "{\"name\":\"x\",\"type\":{\"type\":\"record\",\"name\":\"Deeper\",\"fields\":["
        + "{\"name\":\"y\",\"type\":\"int\"}]}}]}");
    Schema outer = Schema.createRecord("Outer", null, "space", false);
    outer.setFields(Collections.singletonList(new Field("f", inner, null, null)));
    assertEquals(outer, new Schema.Parser().parse(outer.toString()));
  }

  @Test
  public void testNestedNullNamespaceReferencing() {
    Schema inner = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Inner\",\"fields\":[]}");
    Schema outer = Schema.createRecord("Outer", null, "space", false);
    outer.setFields(Arrays.asList(new Field("f1", inner, null, null), new Field("f2", inner, null, null)));
    assertEquals(outer, new Schema.Parser().parse(outer.toString()));
  }

  @Test
  public void testNestedNullNamespaceReferencingWithUnion() {
    Schema inner = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Inner\",\"fields\":[]}");
    Schema innerUnion = Schema.createUnion(Arrays.asList(inner, Schema.create(Type.NULL)));
    Schema outer = Schema.createRecord("Outer", null, "space", false);
    outer.setFields(Arrays.asList(new Field("f1", innerUnion, null, null), new Field("f2", innerUnion, null, null)));
    assertEquals(outer, new Schema.Parser().parse(outer.toString()));
  }

  @Test
  public void testNestedNonNullNamespace1() {
    Schema inner1 = Schema.createEnum("InnerEnum", null, "space", Collections.singletonList("x"));
    Schema inner2 = new Schema.Parser()
        .parse("{\"type\":\"record\",\"namespace\":\"space\",\"name\":" + "\"InnerRecord\",\"fields\":[]}");
    Schema nullOuter = Schema.createRecord("Outer", null, null, false);
    nullOuter.setFields(Arrays.asList(new Field("f1", inner1, null, null), new Field("f2", inner2, null, null)));
    assertEquals(nullOuter, new Schema.Parser().parse(nullOuter.toString()));
  }

  @Test
  public void testNestedNonNullNamespace2() {
    Schema inner1 = Schema.createFixed("InnerFixed", null, "space", 1);
    Schema inner2 = new Schema.Parser()
        .parse("{\"type\":\"record\",\"namespace\":\"space\",\"name\":" + "\"InnerRecord\",\"fields\":[]}");
    Schema nullOuter = Schema.createRecord("Outer", null, null, false);
    nullOuter.setFields(Arrays.asList(new Field("f1", inner1, null, null), new Field("f2", inner2, null, null)));
    assertEquals(nullOuter, new Schema.Parser().parse(nullOuter.toString()));
  }

  @Test
  public void testNullNamespaceAlias() {
    Schema s = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Z\",\"fields\":[]}");
    Schema t = new Schema.Parser()
        .parse("{\"type\":\"record\",\"name\":\"x.Y\",\"aliases\":[\".Z\"]," + "\"fields\":[]}");
    Schema u = Schema.applyAliases(s, t);
    assertEquals("x.Y", u.getFullName());
  }

  @Test
  public void testNullPointer() throws Exception {
    String recordJson = "{\"type\":\"record\", \"name\":\"Test\", \"fields\":"
        + "[{\"name\":\"x\", \"type\":\"string\"}]}";
    Schema schema = new Schema.Parser().parse(recordJson);
    GenericData.Record record = new GenericData.Record(schema);
    assertThrows(NullPointerException.class,
        () -> checkBinary(schema, record, new GenericDatumWriter<>(), new GenericDatumReader<>()));
  }

  private static void checkParseError(String json) {
    try {
      new Schema.Parser().parse(json);
    } catch (SchemaParseException e) {
      return;
    }
    fail("Should not have parsed: " + json);
  }

  private static void checkUnionError(Schema[] branches) {
    List<Schema> branchList = Arrays.asList(branches);
    try {
      Schema.createUnion(branchList);
      fail("Union should not have constructed from: " + branchList);
    } catch (AvroRuntimeException ignored) {
    }
  }

  private static Schema buildUnion(Schema[] branches) {
    List<Schema> branchList = Arrays.asList(branches);
    return Schema.createUnion(branchList);
  }

  /**
   * Makes sure that "doc" tags are transcribed in the schemas. Note that there
   * are docs both for fields and for the records themselves.
   */
  @Test
  public void testDocs() {
    Schema schema = new Schema.Parser().parse(SCHEMA_WITH_DOC_TAGS);
    assertEquals("This is not a world record.", schema.getDoc());
    assertEquals("Inner Fixed", schema.getField("inner_fixed").doc());
    assertEquals("Very Inner Fixed", schema.getField("inner_fixed").schema().getDoc());
    assertEquals("Inner String", schema.getField("inner_string").doc());
    assertEquals("Inner Enum", schema.getField("inner_enum").doc());
    assertEquals("Very Inner Enum", schema.getField("inner_enum").schema().getDoc());
    assertEquals("Inner Union", schema.getField("inner_union").doc());
  }

  @Test
  public void testFieldDocs() {
    String schemaStr = "{\"name\": \"Rec\",\"type\": \"record\",\"fields\" : ["
        + "{\"name\": \"f\", \"type\": \"int\", \"doc\": \"test\"}]}";

    // check field doc is parsed correctly
    Schema schema = new Schema.Parser().parse(schemaStr);
    assertEquals("test", schema.getField("f").doc());

    // check print/read cycle preserves field doc
    schema = new Schema.Parser().parse(schema.toString());
    assertEquals("test", schema.getField("f").doc());
  }

  @Test
  public void testAliases() {
    String t1 = "{\"type\":\"record\",\"name\":\"a.b\",\"fields\":[" + "{\"name\":\"f\",\"type\":\"long\"},"
        + "{\"name\":\"h\",\"type\":\"int\"}]}";
    String t2 = "{\"type\":\"record\",\"name\":\"x.y\",\"aliases\":[\"a.b\"],"
        + "\"fields\":[{\"name\":\"g\",\"type\":\"long\",\"aliases\":[\"f\"]}," + "{\"name\":\"h\",\"type\":\"int\"}]}";
    Schema s1 = new Schema.Parser().parse(t1);
    Schema s2 = new Schema.Parser().parse(t2);

    assertEquals(s1.getAliases(), Collections.emptySet());
    assertEquals(s1.getField("f").aliases(), Collections.emptySet());
    assertEquals(s2.getAliases(), Collections.singleton("a.b"));
    assertEquals(s2.getField("g").aliases(), Collections.singleton("f"));

    Schema s3 = Schema.applyAliases(s1, s2);
    assertNotSame(s2, s3);
    assertEquals(s2, s3);

    t1 = "{\"type\":\"enum\",\"name\":\"a.b\"," + "\"symbols\":[\"x\"]}";
    t2 = "{\"type\":\"enum\",\"name\":\"a.c\",\"aliases\":[\"b\"]," + "\"symbols\":[\"x\"]}";
    s1 = new Schema.Parser().parse(t1);
    s2 = new Schema.Parser().parse(t2);
    s3 = Schema.applyAliases(s1, s2);
    assertNotSame(s2, s3);
    assertEquals(s2, s3);

    t1 = "{\"type\":\"fixed\",\"name\":\"a\"," + "\"size\": 5}";
    t2 = "{\"type\":\"fixed\",\"name\":\"b\",\"aliases\":[\"a\"]," + "\"size\": 5}";
    s1 = new Schema.Parser().parse(t1);
    s2 = new Schema.Parser().parse(t2);
    s3 = Schema.applyAliases(s1, s2);
    assertNotSame(s2, s3);
    assertEquals(s2, s3);
  }

  @Test
  public void testAliasesSelfReferential() {
    String t1 = "{\"type\":\"record\",\"name\":\"a\",\"fields\":[{\"name\":\"f\",\"type\":{\"type\":\"record\",\"name\":\"C\",\"fields\":[{\"name\":\"c\",\"type\":{\"type\":\"array\",\"items\":[\"null\",\"C\"]}}]}}]}";
    String t2 = "{\"type\":\"record\",\"name\":\"x\",\"fields\":[{\"name\":\"f\",\"type\":{\"type\":\"record\",\"name\":\"C\",\"fields\":[{\"name\":\"d\",\"type\":{\"type\":\"array\",\"items\":[\"null\",\"C\"]},\"aliases\":[\"c\"]}]}}],\"aliases\":[\"a\"]}";
    Schema s1 = new Schema.Parser().parse(t1);
    Schema s2 = new Schema.Parser().parse(t2);

    assertEquals(s1.getAliases(), Collections.emptySet());
    assertEquals(s2.getAliases(), Collections.singleton("a"));

    Schema s3 = Schema.applyAliases(s1, s2);
    assertNotSame(s2, s3);
    assertEquals(s2, s3);
  }

  private static void check(File dst, String schemaJson, String defaultJson, Object defaultValue) throws Exception {
    check(dst, schemaJson, defaultJson, defaultValue, true);
  }

  private static void check(File dst, String schemaJson, String defaultJson, Object defaultValue, boolean induce)
      throws Exception {
    check(dst, schemaJson, induce);
    checkDefault(schemaJson, defaultJson, defaultValue);
  }

  private static void check(File dst, String jsonSchema, boolean induce) throws Exception {
    Schema schema = new Schema.Parser().parse(jsonSchema);
    checkProp(schema);
    Object reuse = null;
    for (Object datum : new RandomData(schema, COUNT, true)) {

      if (induce) {
        Schema induced = GenericData.get().induce(datum);
        assertEquals("Induced schema does not match.", schema, induced);
      }

      assertTrue("Datum does not validate against schema " + datum, GenericData.get().validate(schema, datum));

      checkBinary(schema, datum, new GenericDatumWriter<>(), new GenericDatumReader<>(), null);
      reuse = checkBinary(schema, datum, new GenericDatumWriter<>(), new GenericDatumReader<>(), reuse);
      checkDirectBinary(schema, datum, new GenericDatumWriter<>(), new GenericDatumReader<>());
      checkBlockingBinary(schema, datum, new GenericDatumWriter<>(), new GenericDatumReader<>());
      checkJson(schema, datum, new GenericDatumWriter<>(), new GenericDatumReader<>());

      // Check that we can generate the code for every schema we see.
      TestSpecificCompiler.assertCompiles(dst, schema, false);

      // Check that we can read/write the json of every schema we see.
      checkBinaryJson(jsonSchema);
    }
  }

  private static void checkProp(Schema s0) throws Exception {
    if (s0.getType().equals(Schema.Type.UNION))
      return; // unions have no props
    assertNull(s0.getProp("foo"));
    Schema s1 = new Schema.Parser().parse(s0.toString());
    s1.addProp("foo", "bar");
    assertEquals("bar", s1.getProp("foo"));
    assertNotEquals(s0, s1);
    Schema s2 = new Schema.Parser().parse(s1.toString());
    assertEquals("bar", s2.getProp("foo"));
    assertEquals(s1, s2);
    assertNotEquals(s0, s2);
  }

  public static void checkBinary(Schema schema, Object datum, DatumWriter<Object> writer, DatumReader<Object> reader)
      throws IOException {
    checkBinary(schema, datum, writer, reader, null);
  }

  public static Object checkBinary(Schema schema, Object datum, DatumWriter<Object> writer, DatumReader<Object> reader,
      Object reuse) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.setSchema(schema);
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    writer.write(datum, encoder);
    encoder.flush();
    byte[] data = out.toByteArray();

    reader.setSchema(schema);

    Object decoded = reader.read(reuse, DecoderFactory.get().binaryDecoder(data, null));

    assertEquals("Decoded data does not match.", datum, decoded);
    return decoded;
  }

  public static void checkDirectBinary(Schema schema, Object datum, DatumWriter<Object> writer,
      DatumReader<Object> reader) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.setSchema(schema);
    Encoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
    writer.write(datum, encoder);
    // no flush for direct
    byte[] data = out.toByteArray();

    reader.setSchema(schema);

    Object decoded = reader.read(null, DecoderFactory.get().binaryDecoder(data, null));

    assertEquals("Decoded data does not match.", datum, decoded);
  }

  public static void checkBlockingBinary(Schema schema, Object datum, DatumWriter<Object> writer,
      DatumReader<Object> reader) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.setSchema(schema);
    Encoder encoder = EncoderFactory.get().blockingBinaryEncoder(out, null);
    writer.write(datum, encoder);
    encoder.flush();
    byte[] data = out.toByteArray();

    reader.setSchema(schema);

    Object decoded = reader.read(null, DecoderFactory.get().binaryDecoder(data, null));

    assertEquals("Decoded data does not match.", datum, decoded);
  }

  private static void checkJson(Schema schema, Object datum, DatumWriter<Object> writer, DatumReader<Object> reader)
      throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().jsonEncoder(schema, out);
    writer.setSchema(schema);
    writer.write(datum, encoder);
    writer.write(datum, encoder);
    encoder.flush();
    byte[] data = out.toByteArray();

    reader.setSchema(schema);
    Decoder decoder = DecoderFactory.get().jsonDecoder(schema, new ByteArrayInputStream(data));
    Object decoded = reader.read(null, decoder);
    assertEquals("Decoded data does not match.", datum, decoded);

    decoded = reader.read(decoded, decoder);
    assertEquals("Decoded data does not match.", datum, decoded);
  }

  private static void checkJson(Schema schema, Object datum, String json) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().jsonEncoder(schema, out);
    DatumWriter<Object> writer = new GenericDatumWriter<>();
    writer.setSchema(schema);
    writer.write(datum, encoder);
    encoder.flush();
    byte[] data = out.toByteArray();

    String encoded = new String(data, StandardCharsets.UTF_8);
    assertEquals("Encoded data does not match.", json, encoded);

    DatumReader<Object> reader = new GenericDatumReader<>();
    reader.setSchema(schema);
    Object decoded = reader.read(null, DecoderFactory.get().jsonDecoder(schema, new ByteArrayInputStream(data)));

    assertEquals("Decoded data does not match.", datum, decoded);
  }

  public static void checkBinaryJson(String json) throws Exception {
    Object node = Json.parseJson(json);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DatumWriter<Object> writer = new Json.ObjectWriter();
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    encoder = EncoderFactory.get().validatingEncoder(Json.SCHEMA, encoder);
    writer.write(node, encoder);
    encoder.flush();
    byte[] bytes = out.toByteArray();

    DatumReader<Object> reader = new Json.ObjectReader();
    Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
    decoder = DecoderFactory.get().validatingDecoder(Json.SCHEMA, decoder);
    Object decoded = reader.read(null, decoder);

    assertEquals("Decoded json does not match.", Json.toString(node), Json.toString(decoded));
  }

  private static final Schema ACTUAL = // an empty record schema
      new Schema.Parser().parse("{\"type\":\"record\", \"name\":\"Foo\", \"fields\":[]}");

  private static void checkDefault(String schemaJson, String defaultJson, Object defaultValue) throws Exception {
    String recordJson = "{\"type\":\"record\", \"name\":\"Foo\", \"fields\":[{\"name\":\"f\", " + "\"type\":"
        + schemaJson + ", " + "\"default\":" + defaultJson + "}]}";
    Schema expected = new Schema.Parser().parse(recordJson);
    DatumReader<Object> in = new GenericDatumReader<>(ACTUAL, expected);
    GenericData.Record record = (GenericData.Record) in.read(null,
        DecoderFactory.get().binaryDecoder(new byte[0], null));
    assertEquals("Wrong default.", defaultValue, record.get("f"));
    assertEquals("Wrong toString", expected, new Schema.Parser().parse(expected.toString()));
  }

  private static void checkValidateDefaults(String schemaJson, String defaultJson) {
    try {
      Schema.Parser parser = new Schema.Parser();
      String recordJson = "{\"type\":\"record\", \"name\":\"Foo\", \"fields\":[{\"name\":\"f\", " + "\"type\":"
          + schemaJson + ", " + "\"default\":" + defaultJson + "}]}";
      parser.parse(recordJson);
      fail("Schema of type " + schemaJson + " should not have default " + defaultJson);
    } catch (AvroTypeException ignored) {
    }
  }

  @Test(expected = AvroTypeException.class)
  public void testNoDefaultField() throws Exception {
    Schema expected = new Schema.Parser()
        .parse("{\"type\":\"record\", \"name\":\"Foo\", \"fields\":" + "[{\"name\":\"f\", \"type\": \"string\"}]}");
    DatumReader<Object> in = new GenericDatumReader<>(ACTUAL, expected);
    in.read(null, DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(new byte[0]), null));
  }

  @Test
  public void testEnumMismatch() throws Exception {
    Schema actual = new Schema.Parser().parse("{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"X\",\"Y\"]}");
    Schema expected = new Schema.Parser().parse("{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"Y\",\"Z\"]}");
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DatumWriter<Object> writer = new GenericDatumWriter<>(actual);
    Encoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
    writer.write(new GenericData.EnumSymbol(actual, "Y"), encoder);
    writer.write(new GenericData.EnumSymbol(actual, "X"), encoder);
    encoder.flush();
    byte[] data = out.toByteArray();
    Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
    DatumReader<String> in = new GenericDatumReader<>(actual, expected);
    assertEquals("Wrong value", new GenericData.EnumSymbol(expected, "Y"), in.read(null, decoder));
    try {
      in.read(null, decoder);
      fail("Should have thrown exception.");
    } catch (AvroTypeException e) {
      // expected
    }
  }

  @Test(expected = AvroTypeException.class)
  public void testRecordWithPrimitiveName() {
    new Schema.Parser().parse("{\"type\":\"record\", \"name\":\"string\", \"fields\": []}");
  }

  @Test(expected = AvroTypeException.class)
  public void testEnumWithPrimitiveName() {
    new Schema.Parser().parse("{\"type\":\"enum\", \"name\":\"null\", \"symbols\": [\"A\"]}");
  }

  private static Schema enumSchema() {
    return new Schema.Parser().parse("{ \"type\": \"enum\", \"name\": \"e\", " + "\"symbols\": [\"a\", \"b\"]}");
  }

  @Test(expected = AvroRuntimeException.class)
  public void testImmutability1() {
    Schema s = enumSchema();
    s.addProp("p1", "1");
    s.addProp("p1", "2");
  }

  @Test(expected = AvroRuntimeException.class)
  public void testImmutability2() {
    Schema s = enumSchema();
    s.addProp("p1", null);
  }

  private static List<String> lockedArrayList() {
    return new Schema.LockableArrayList<>(Arrays.asList("a", "b", "c")).lock();
  }

  @Test(expected = IllegalStateException.class)
  public void testLockedArrayList1() {
    lockedArrayList().add("p");
  }

  @Test(expected = IllegalStateException.class)
  public void testLockedArrayList2() {
    lockedArrayList().remove("a");
  }

  @Test(expected = IllegalStateException.class)
  public void testLockedArrayList3() {
    lockedArrayList().addAll(Collections.singletonList("p"));
  }

  @Test(expected = IllegalStateException.class)
  public void testLockedArrayList4() {
    lockedArrayList().addAll(0, Collections.singletonList("p"));
  }

  @Test(expected = IllegalStateException.class)
  public void testLockedArrayList5() {
    lockedArrayList().removeAll(Collections.singletonList("a"));
  }

  @Test(expected = IllegalStateException.class)
  public void testLockedArrayList6() {
    lockedArrayList().retainAll(Collections.singletonList("a"));
  }

  @Test(expected = IllegalStateException.class)
  public void testLockedArrayList7() {
    lockedArrayList().clear();
  }

  @Test(expected = IllegalStateException.class)
  public void testLockedArrayList8() {
    lockedArrayList().iterator().remove();
  }

  @Test(expected = IllegalStateException.class)
  public void testLockedArrayList9() {
    Iterator<String> it = lockedArrayList().iterator();
    it.next();
    it.remove();
  }

  @Test(expected = IllegalStateException.class)
  public void testLockedArrayList10() {
    lockedArrayList().remove(1);
  }

  @Test
  public void testNames_GetWithInheritedNamespace() {
    Schema schema = Schema.create(Type.STRING);
    Schema.Names names = new Schema.Names("space");
    names.put(new Schema.Name("Name", "space"), schema);

    assertEquals(schema, names.get(new Schema.Name("Name", "space")));
    assertEquals(schema, names.get("Name"));
  }

  @Test
  public void testNames_GetWithNullNamespace() {
    Schema schema = Schema.create(Type.STRING);
    Schema.Names names = new Schema.Names("space");
    names.put(new Schema.Name("Name", ""), schema);

    assertEquals(schema, names.get(new Schema.Name("Name", "")));
    assertEquals(schema, names.get("Name"));
  }

  @Test
  public void testNames_GetNotFound() {
    Schema.Names names = new Schema.Names("space");
    names.put(new Schema.Name("Name", "otherspace"), Schema.create(Type.STRING));

    assertNull(names.get("Name"));
  }
}
