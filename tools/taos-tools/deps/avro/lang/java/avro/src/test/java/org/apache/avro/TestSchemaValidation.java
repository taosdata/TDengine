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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.avro;

import static org.apache.avro.TestSchemas.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.avro.reflect.ReflectData;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestSchemaValidation {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  /** Collection of reader/writer schema pair that are compatible. */
  public static final List<ReaderWriter> COMPATIBLE_READER_WRITER_TEST_CASES = list(
      new ReaderWriter(BOOLEAN_SCHEMA, BOOLEAN_SCHEMA),

      new ReaderWriter(INT_SCHEMA, INT_SCHEMA),

      new ReaderWriter(LONG_SCHEMA, INT_SCHEMA), new ReaderWriter(LONG_SCHEMA, LONG_SCHEMA),

      // Avro spec says INT/LONG can be promoted to FLOAT/DOUBLE.
      // This is arguable as this causes a loss of precision.
      new ReaderWriter(FLOAT_SCHEMA, INT_SCHEMA), new ReaderWriter(FLOAT_SCHEMA, LONG_SCHEMA),
      new ReaderWriter(DOUBLE_SCHEMA, LONG_SCHEMA),

      new ReaderWriter(DOUBLE_SCHEMA, INT_SCHEMA), new ReaderWriter(DOUBLE_SCHEMA, FLOAT_SCHEMA),

      new ReaderWriter(STRING_SCHEMA, STRING_SCHEMA),

      new ReaderWriter(BYTES_SCHEMA, BYTES_SCHEMA),

      new ReaderWriter(INT_ARRAY_SCHEMA, INT_ARRAY_SCHEMA), new ReaderWriter(LONG_ARRAY_SCHEMA, INT_ARRAY_SCHEMA),
      new ReaderWriter(INT_MAP_SCHEMA, INT_MAP_SCHEMA), new ReaderWriter(LONG_MAP_SCHEMA, INT_MAP_SCHEMA),

      new ReaderWriter(ENUM1_AB_SCHEMA, ENUM1_AB_SCHEMA), new ReaderWriter(ENUM1_ABC_SCHEMA, ENUM1_AB_SCHEMA),

      // String-to/from-bytes, introduced in Avro 1.7.7
      new ReaderWriter(STRING_SCHEMA, BYTES_SCHEMA), new ReaderWriter(BYTES_SCHEMA, STRING_SCHEMA),

      // Tests involving unions:
      new ReaderWriter(EMPTY_UNION_SCHEMA, EMPTY_UNION_SCHEMA), new ReaderWriter(INT_UNION_SCHEMA, INT_UNION_SCHEMA),
      new ReaderWriter(INT_STRING_UNION_SCHEMA, STRING_INT_UNION_SCHEMA),
      new ReaderWriter(INT_UNION_SCHEMA, EMPTY_UNION_SCHEMA), new ReaderWriter(LONG_UNION_SCHEMA, INT_UNION_SCHEMA),
      new ReaderWriter(FLOAT_UNION_SCHEMA, INT_UNION_SCHEMA), new ReaderWriter(FLOAT_UNION_SCHEMA, LONG_UNION_SCHEMA),
      new ReaderWriter(DOUBLE_UNION_SCHEMA, INT_UNION_SCHEMA), new ReaderWriter(LONG_UNION_SCHEMA, EMPTY_UNION_SCHEMA),
      new ReaderWriter(DOUBLE_UNION_SCHEMA, LONG_UNION_SCHEMA),
      new ReaderWriter(FLOAT_UNION_SCHEMA, EMPTY_UNION_SCHEMA),
      new ReaderWriter(DOUBLE_UNION_SCHEMA, FLOAT_UNION_SCHEMA),
      new ReaderWriter(STRING_UNION_SCHEMA, EMPTY_UNION_SCHEMA),
      new ReaderWriter(STRING_UNION_SCHEMA, BYTES_UNION_SCHEMA),
      new ReaderWriter(BYTES_UNION_SCHEMA, EMPTY_UNION_SCHEMA),
      new ReaderWriter(BYTES_UNION_SCHEMA, STRING_UNION_SCHEMA),
      new ReaderWriter(DOUBLE_UNION_SCHEMA, INT_FLOAT_UNION_SCHEMA),

      new ReaderWriter(NULL_INT_ARRAY_UNION_SCHEMA, INT_ARRAY_SCHEMA),
      new ReaderWriter(NULL_INT_MAP_UNION_SCHEMA, INT_MAP_SCHEMA),

      // Readers capable of reading all branches of a union are compatible
      new ReaderWriter(FLOAT_SCHEMA, INT_FLOAT_UNION_SCHEMA), new ReaderWriter(LONG_SCHEMA, INT_LONG_UNION_SCHEMA),
      new ReaderWriter(DOUBLE_SCHEMA, INT_FLOAT_UNION_SCHEMA),
      new ReaderWriter(DOUBLE_SCHEMA, INT_LONG_FLOAT_DOUBLE_UNION_SCHEMA),

      // Special case of singleton unions:
      new ReaderWriter(FLOAT_SCHEMA, FLOAT_UNION_SCHEMA), new ReaderWriter(INT_UNION_SCHEMA, INT_SCHEMA),
      new ReaderWriter(INT_SCHEMA, INT_UNION_SCHEMA),

      // Tests involving records:
      new ReaderWriter(EMPTY_RECORD1, EMPTY_RECORD1), new ReaderWriter(EMPTY_RECORD1, A_INT_RECORD1),

      new ReaderWriter(A_INT_RECORD1, A_INT_RECORD1), new ReaderWriter(A_DINT_RECORD1, A_INT_RECORD1),
      new ReaderWriter(A_DINT_RECORD1, A_DINT_RECORD1), new ReaderWriter(A_INT_RECORD1, A_DINT_RECORD1),

      new ReaderWriter(A_LONG_RECORD1, A_INT_RECORD1),

      new ReaderWriter(A_INT_RECORD1, A_INT_B_INT_RECORD1), new ReaderWriter(A_DINT_RECORD1, A_INT_B_INT_RECORD1),

      new ReaderWriter(A_INT_B_DINT_RECORD1, A_INT_RECORD1), new ReaderWriter(A_DINT_B_DINT_RECORD1, EMPTY_RECORD1),
      new ReaderWriter(A_DINT_B_DINT_RECORD1, A_INT_RECORD1),
      new ReaderWriter(A_INT_B_INT_RECORD1, A_DINT_B_DINT_RECORD1),

      // The SchemaValidator, unlike the SchemaCompatibility class, cannot cope with
      // recursive schemas
      // See AVRO-2074
      // new ReaderWriter(INT_LIST_RECORD, INT_LIST_RECORD),
      // new ReaderWriter(LONG_LIST_RECORD, LONG_LIST_RECORD),
      // new ReaderWriter(LONG_LIST_RECORD, INT_LIST_RECORD),

      new ReaderWriter(NULL_SCHEMA, NULL_SCHEMA),

      // This is comparing two records that have an inner array of records with
      // different namespaces.
      new ReaderWriter(NS_RECORD1, NS_RECORD2));

  /** Collection of reader/writer schema pair that are incompatible. */
  public static final List<ReaderWriter> INCOMPATIBLE_READER_WRITER_TEST_CASES = list(
      new ReaderWriter(NULL_SCHEMA, INT_SCHEMA), new ReaderWriter(NULL_SCHEMA, LONG_SCHEMA),

      new ReaderWriter(BOOLEAN_SCHEMA, INT_SCHEMA),

      new ReaderWriter(INT_SCHEMA, NULL_SCHEMA), new ReaderWriter(INT_SCHEMA, BOOLEAN_SCHEMA),
      new ReaderWriter(INT_SCHEMA, LONG_SCHEMA), new ReaderWriter(INT_SCHEMA, FLOAT_SCHEMA),
      new ReaderWriter(INT_SCHEMA, DOUBLE_SCHEMA),

      new ReaderWriter(LONG_SCHEMA, FLOAT_SCHEMA), new ReaderWriter(LONG_SCHEMA, DOUBLE_SCHEMA),

      new ReaderWriter(FLOAT_SCHEMA, DOUBLE_SCHEMA),

      new ReaderWriter(STRING_SCHEMA, BOOLEAN_SCHEMA), new ReaderWriter(STRING_SCHEMA, INT_SCHEMA),

      new ReaderWriter(BYTES_SCHEMA, NULL_SCHEMA), new ReaderWriter(BYTES_SCHEMA, INT_SCHEMA),

      new ReaderWriter(INT_ARRAY_SCHEMA, LONG_ARRAY_SCHEMA), new ReaderWriter(INT_MAP_SCHEMA, INT_ARRAY_SCHEMA),
      new ReaderWriter(INT_ARRAY_SCHEMA, INT_MAP_SCHEMA), new ReaderWriter(INT_MAP_SCHEMA, LONG_MAP_SCHEMA),

      new ReaderWriter(ENUM1_AB_SCHEMA, ENUM1_ABC_SCHEMA), new ReaderWriter(ENUM1_BC_SCHEMA, ENUM1_ABC_SCHEMA),

      new ReaderWriter(ENUM1_AB_SCHEMA, ENUM2_AB_SCHEMA), new ReaderWriter(INT_SCHEMA, ENUM2_AB_SCHEMA),
      new ReaderWriter(ENUM2_AB_SCHEMA, INT_SCHEMA),

      // Tests involving unions:
      new ReaderWriter(INT_UNION_SCHEMA, INT_STRING_UNION_SCHEMA),
      new ReaderWriter(STRING_UNION_SCHEMA, INT_STRING_UNION_SCHEMA),
      new ReaderWriter(FLOAT_SCHEMA, INT_LONG_FLOAT_DOUBLE_UNION_SCHEMA),
      new ReaderWriter(LONG_SCHEMA, INT_FLOAT_UNION_SCHEMA), new ReaderWriter(INT_SCHEMA, INT_FLOAT_UNION_SCHEMA),

      new ReaderWriter(EMPTY_RECORD2, EMPTY_RECORD1), new ReaderWriter(A_INT_RECORD1, EMPTY_RECORD1),
      new ReaderWriter(A_INT_B_DINT_RECORD1, EMPTY_RECORD1),

      new ReaderWriter(INT_LIST_RECORD, LONG_LIST_RECORD),

      new ReaderWriter(NULL_SCHEMA, INT_SCHEMA));

  SchemaValidatorBuilder builder = new SchemaValidatorBuilder();
  Schema rec = SchemaBuilder.record("test.Rec").fields().name("a").type().intType().intDefault(1).name("b").type()
      .longType().noDefault().endRecord();
  Schema rec2 = SchemaBuilder.record("test.Rec").fields().name("a").type().intType().intDefault(1).name("b").type()
      .longType().noDefault().name("c").type().intType().intDefault(0).endRecord();
  Schema rec3 = SchemaBuilder.record("test.Rec").fields().name("b").type().longType().noDefault().name("c").type()
      .intType().intDefault(0).endRecord();
  Schema rec4 = SchemaBuilder.record("test.Rec").fields().name("b").type().longType().noDefault().name("c").type()
      .intType().noDefault().endRecord();
  Schema rec5 = SchemaBuilder.record("test.Rec").fields().name("a").type().stringType().stringDefault("") // different
                                                                                                          // type from
                                                                                                          // original
      .name("b").type().longType().noDefault().name("c").type().intType().intDefault(0).endRecord();

  @Test
  public void testAllTypes() throws SchemaValidationException {
    Schema s = SchemaBuilder.record("r").fields().requiredBoolean("boolF").requiredInt("intF").requiredLong("longF")
        .requiredFloat("floatF").requiredDouble("doubleF").requiredString("stringF").requiredBytes("bytesF")
        .name("fixedF1").type().fixed("F1").size(1).noDefault().name("enumF").type().enumeration("E1").symbols("S")
        .noDefault().name("mapF").type().map().values().stringType().noDefault().name("arrayF").type().array().items()
        .stringType().noDefault().name("recordF").type().record("inner").fields().name("f").type().intType().noDefault()
        .endRecord().noDefault().optionalBoolean("boolO").endRecord();
    testValidatorPasses(builder.mutualReadStrategy().validateLatest(), s, s);
  }

  @Test
  public void testReadOnePrior() throws SchemaValidationException {
    testValidatorPasses(builder.canReadStrategy().validateLatest(), rec3, rec);
    testValidatorPasses(builder.canReadStrategy().validateLatest(), rec5, rec3);
    testValidatorFails(builder.canReadStrategy().validateLatest(), rec4, rec);
  }

  @Test
  public void testReadAllPrior() throws SchemaValidationException {
    testValidatorPasses(builder.canReadStrategy().validateAll(), rec3, rec, rec2);
    testValidatorFails(builder.canReadStrategy().validateAll(), rec4, rec, rec2, rec3);
    testValidatorFails(builder.canReadStrategy().validateAll(), rec5, rec, rec2, rec3);
  }

  @Test
  public void testOnePriorCanRead() throws SchemaValidationException {
    testValidatorPasses(builder.canBeReadStrategy().validateLatest(), rec, rec3);
    testValidatorFails(builder.canBeReadStrategy().validateLatest(), rec, rec4);
  }

  @Test
  public void testAllPriorCanRead() throws SchemaValidationException {
    testValidatorPasses(builder.canBeReadStrategy().validateAll(), rec, rec3, rec2);
    testValidatorFails(builder.canBeReadStrategy().validateAll(), rec, rec4, rec3, rec2);
  }

  @Test
  public void testOnePriorCompatible() throws SchemaValidationException {
    testValidatorPasses(builder.mutualReadStrategy().validateLatest(), rec, rec3);
    testValidatorFails(builder.mutualReadStrategy().validateLatest(), rec, rec4);
  }

  @Test
  public void testAllPriorCompatible() throws SchemaValidationException {
    testValidatorPasses(builder.mutualReadStrategy().validateAll(), rec, rec3, rec2);
    testValidatorFails(builder.mutualReadStrategy().validateAll(), rec, rec4, rec3, rec2);
  }

  @Test(expected = AvroRuntimeException.class)
  public void testInvalidBuild() {
    builder.strategy(null).validateAll();
  }

  public static class Point {
    double x;
    double y;
  }

  public static class Circle {
    Point center;
    double radius;
  }

  public static final Schema circleSchema = SchemaBuilder.record("Circle").fields().name("center").type()
      .record("Point").fields().requiredDouble("x").requiredDouble("y").endRecord().noDefault().requiredDouble("radius")
      .endRecord();

  public static final Schema circleSchemaDifferentNames = SchemaBuilder.record("crcl").fields().name("center").type()
      .record("pt").fields().requiredDouble("x").requiredDouble("y").endRecord().noDefault().requiredDouble("radius")
      .endRecord();

  @Test
  public void testReflectMatchStructure() throws SchemaValidationException {
    testValidatorPasses(builder.canBeReadStrategy().validateAll(), circleSchemaDifferentNames,
        ReflectData.get().getSchema(Circle.class));
  }

  @Test
  public void testReflectWithAllowNullMatchStructure() throws SchemaValidationException {
    testValidatorPasses(builder.canBeReadStrategy().validateAll(), circleSchemaDifferentNames,
        ReflectData.AllowNull.get().getSchema(Circle.class));
  }

  @Test
  public void testUnionWithIncompatibleElements() throws SchemaValidationException {
    Schema union1 = Schema.createUnion(Collections.singletonList(rec));
    Schema union2 = Schema.createUnion(Collections.singletonList(rec4));
    testValidatorFails(builder.canReadStrategy().validateAll(), union2, union1);
  }

  @Test
  public void testUnionWithCompatibleElements() throws SchemaValidationException {
    Schema union1 = Schema.createUnion(Collections.singletonList(rec));
    Schema union2 = Schema.createUnion(Collections.singletonList(rec3));
    testValidatorPasses(builder.canReadStrategy().validateAll(), union2, union1);
  }

  @Test
  public void testSchemaCompatibilitySuccesses() throws SchemaValidationException {
    // float-union-to-int/long-union does not work...
    // and neither does recursive types
    for (ReaderWriter tc : COMPATIBLE_READER_WRITER_TEST_CASES) {
      testValidatorPasses(builder.canReadStrategy().validateAll(), tc.getReader(), tc.getWriter());
    }
  }

  @Test
  public void testSchemaCompatibilityFailures() throws SchemaValidationException {
    for (ReaderWriter tc : INCOMPATIBLE_READER_WRITER_TEST_CASES) {
      Schema reader = tc.getReader();
      Schema writer = tc.getWriter();
      expectedException.expect(SchemaValidationException.class);
      expectedException.expectMessage("Unable to read schema: \n" + writer.toString());
      SchemaValidator validator = builder.canReadStrategy().validateAll();
      validator.validate(reader, Collections.singleton(writer));
    }
  }

  private void testValidatorPasses(SchemaValidator validator, Schema schema, Schema... prev)
      throws SchemaValidationException {
    ArrayList<Schema> prior = new ArrayList<>();
    for (int i = prev.length - 1; i >= 0; i--) {
      prior.add(prev[i]);
    }
    validator.validate(schema, prior);
  }

  private void testValidatorFails(SchemaValidator validator, Schema schemaFails, Schema... prev)
      throws SchemaValidationException {
    ArrayList<Schema> prior = new ArrayList<>();
    for (int i = prev.length - 1; i >= 0; i--) {
      prior.add(prev[i]);
    }
    boolean threw = false;
    try {
      // should fail
      validator.validate(schemaFails, prior);
    } catch (SchemaValidationException sve) {
      threw = true;
    }
    Assert.assertTrue(threw);
  }

  public static final org.apache.avro.Schema recursiveSchema = new org.apache.avro.Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"Node\",\"namespace\":\"avro\",\"fields\":[{\"name\":\"value\",\"type\":[\"null\",\"Node\"],\"default\":null}]}");

  /**
   * Unit test to verify that recursive schemas can be validated. See AVRO-2122.
   */
  @Test
  public void testRecursiveSchemaValidation() throws SchemaValidationException {
    // before AVRO-2122, this would cause a StackOverflowError
    final SchemaValidator backwardValidator = builder.canReadStrategy().validateLatest();
    backwardValidator.validate(recursiveSchema, Collections.singletonList(recursiveSchema));
  }
}
