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
package org.apache.avro.io.parsing;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.SchemaValidatorBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/** ResolvingGrammarGenerator tests that are not Parameterized. */
public class TestResolvingGrammarGenerator2 {
  @Test
  public void testFixed() throws java.io.IOException {
    new ResolvingGrammarGenerator().generate(Schema.createFixed("MyFixed", null, null, 10),
        Schema.create(Schema.Type.BYTES));
    new ResolvingGrammarGenerator().generate(Schema.create(Schema.Type.BYTES),
        Schema.createFixed("MyFixed", null, null, 10));
  }

  Schema point2dFullname = SchemaBuilder.record("Point").namespace("written").fields().requiredDouble("x")
      .requiredDouble("y").endRecord();

  Schema point3dNoDefault = SchemaBuilder.record("Point").fields().requiredDouble("x").requiredDouble("y")
      .requiredDouble("z").endRecord();

  Schema point2d = SchemaBuilder.record("Point2D").fields().requiredDouble("x").requiredDouble("y").endRecord();

  Schema point3d = SchemaBuilder.record("Point3D").fields().requiredDouble("x").requiredDouble("y").name("z").type()
      .doubleType().doubleDefault(0.0).endRecord();

  Schema point3dMatchName = SchemaBuilder.record("Point").fields().requiredDouble("x").requiredDouble("y").name("z")
      .type().doubleType().doubleDefault(0.0).endRecord();

  @Test(expected = SchemaValidationException.class)
  public void testUnionResolutionNoStructureMatch() throws Exception {
    // there is a short name match, but the structure does not match
    Schema read = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), point3dNoDefault));

    new SchemaValidatorBuilder().canBeReadStrategy().validateAll().validate(point2dFullname,
        Collections.singletonList(read));
  }

  @Test
  public void testUnionResolutionFirstStructureMatch2d() throws Exception {
    // multiple structure matches with no short or full name matches
    Schema read = Schema
        .createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), point3dNoDefault, point2d, point3d));

    Symbol grammar = new ResolvingGrammarGenerator().generate(point2dFullname, read);
    Assert.assertTrue(grammar.production[1] instanceof Symbol.UnionAdjustAction);

    Symbol.UnionAdjustAction action = (Symbol.UnionAdjustAction) grammar.production[1];
    Assert.assertEquals(2, action.rindex);
  }

  @Test
  public void testUnionResolutionFirstStructureMatch3d() throws Exception {
    // multiple structure matches with no short or full name matches
    Schema read = Schema
        .createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), point3dNoDefault, point3d, point2d));

    Symbol grammar = new ResolvingGrammarGenerator().generate(point2dFullname, read);
    Assert.assertTrue(grammar.production[1] instanceof Symbol.UnionAdjustAction);

    Symbol.UnionAdjustAction action = (Symbol.UnionAdjustAction) grammar.production[1];
    Assert.assertEquals(2, action.rindex);
  }

  @Test
  public void testUnionResolutionNamedStructureMatch() throws Exception {
    // multiple structure matches with a short name match
    Schema read = Schema
        .createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), point2d, point3dMatchName, point3d));

    Symbol grammar = new ResolvingGrammarGenerator().generate(point2dFullname, read);
    Assert.assertTrue(grammar.production[1] instanceof Symbol.UnionAdjustAction);

    Symbol.UnionAdjustAction action = (Symbol.UnionAdjustAction) grammar.production[1];
    Assert.assertEquals(2, action.rindex);
  }

  @Test
  public void testUnionResolutionFullNameMatch() throws Exception {
    // there is a full name match, so it should be chosen
    Schema read = Schema.createUnion(
        Arrays.asList(Schema.create(Schema.Type.NULL), point2d, point3dMatchName, point3d, point2dFullname));

    Symbol grammar = new ResolvingGrammarGenerator().generate(point2dFullname, read);
    Assert.assertTrue(grammar.production[1] instanceof Symbol.UnionAdjustAction);

    Symbol.UnionAdjustAction action = (Symbol.UnionAdjustAction) grammar.production[1];
    Assert.assertEquals(4, action.rindex);
  }

  @Test
  public void testAvro2702StringProperties() throws IOException {

    // Create a nested record schema with string fields at two levels.
    Schema inner = SchemaBuilder.builder().record("B").fields().requiredString("b1").endRecord();
    Schema outer = SchemaBuilder.builder().record("A").fields().requiredString("a1").name("inner").type().unionOf()
        .nullType().and().type(inner).endUnion().noDefault().endRecord();

    // Make a copy with the two string fields annotated.
    Schema outer2 = new Schema.Parser().parse(outer.toString());
    outer2.getField("a1").schema().addProp(GenericData.STRING_PROP, "String");
    Schema inner2 = outer2.getField("inner").schema().getTypes().get(1);
    inner2.getField("b1").schema().addProp(GenericData.STRING_PROP, "String");

    // The two schemas are not the same, but they serialize to the same.
    assertThat(outer, not(outer2));

    // This is a serialized record.
    byte[] serialized = { 2, 'a', // a1 is a one character string
        2, // Pick the non-null UNION branch and
        2, 'b' // Another one character string
    };

    GenericRecord out = null;
    try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized)) {
      Decoder decoder = DecoderFactory.get().binaryDecoder(bais, null);
      DatumReader<GenericRecord> r = new GenericDatumReader<>(outer, outer2, GenericData.get());
      out = r.read(null, decoder);
    }

    // Assert that the two fields were read and are of type String.
    assertThat(out.get("a1"), instanceOf(String.class));
    assertThat(((GenericRecord) out.get("inner")).get("b1"), instanceOf(String.class));
  }

}
