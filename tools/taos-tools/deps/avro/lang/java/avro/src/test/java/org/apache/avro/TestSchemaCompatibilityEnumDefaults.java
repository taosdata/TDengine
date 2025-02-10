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

import static org.apache.avro.TestSchemas.ENUM1_ABC_SCHEMA;
import static org.apache.avro.TestSchemas.ENUM1_AB_SCHEMA;
import static org.apache.avro.TestSchemas.ENUM2_AB_SCHEMA;
import static org.apache.avro.TestSchemas.ENUM_ABC_ENUM_DEFAULT_A_SCHEMA;
import static org.apache.avro.TestSchemas.ENUM_AB_ENUM_DEFAULT_A_SCHEMA;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestSchemaCompatibilityEnumDefaults {
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testEnumDefaultNotAppliedWhenWriterFieldMissing() throws Exception {
    expectedException.expect(AvroTypeException.class);
    expectedException.expectMessage("Found Record1, expecting Record1, missing required field field1");

    Schema writerSchema = SchemaBuilder.record("Record1").fields().name("field2").type(ENUM2_AB_SCHEMA).noDefault()
        .endRecord();

    Schema readerSchema = SchemaBuilder.record("Record1").fields().name("field1").type(ENUM_AB_ENUM_DEFAULT_A_SCHEMA)
        .noDefault().endRecord();

    GenericRecord datum = new GenericData.Record(writerSchema);
    datum.put("field2", new GenericData.EnumSymbol(writerSchema, "B"));
    serializeWithWriterThenDeserializeWithReader(writerSchema, datum, readerSchema);
  }

  @Test
  public void testEnumDefaultAppliedWhenNoFieldDefaultDefined() throws Exception {
    Schema writerSchema = SchemaBuilder.record("Record1").fields().name("field1").type(ENUM_ABC_ENUM_DEFAULT_A_SCHEMA)
        .noDefault().endRecord();

    Schema readerSchema = SchemaBuilder.record("Record1").fields().name("field1").type(ENUM_AB_ENUM_DEFAULT_A_SCHEMA)
        .noDefault().endRecord();

    GenericRecord datum = new GenericData.Record(writerSchema);
    datum.put("field1", new GenericData.EnumSymbol(writerSchema, "C"));
    GenericRecord decodedDatum = serializeWithWriterThenDeserializeWithReader(writerSchema, datum, readerSchema);
    // The A is the Enum fallback value.
    assertEquals("A", decodedDatum.get("field1").toString());
  }

  @Test
  public void testEnumDefaultNotAppliedWhenCompatibleSymbolIsFound() throws Exception {
    Schema writerSchema = SchemaBuilder.record("Record1").fields().name("field1").type(ENUM_ABC_ENUM_DEFAULT_A_SCHEMA)
        .noDefault().endRecord();

    Schema readerSchema = SchemaBuilder.record("Record1").fields().name("field1").type(ENUM_AB_ENUM_DEFAULT_A_SCHEMA)
        .noDefault().endRecord();

    GenericRecord datum = new GenericData.Record(writerSchema);
    datum.put("field1", new GenericData.EnumSymbol(writerSchema, "B"));
    GenericRecord decodedDatum = serializeWithWriterThenDeserializeWithReader(writerSchema, datum, readerSchema);
    assertEquals("B", decodedDatum.get("field1").toString());
  }

  @Test
  public void testEnumDefaultAppliedWhenFieldDefaultDefined() throws Exception {
    Schema writerSchema = SchemaBuilder.record("Record1").fields().name("field1").type(ENUM_ABC_ENUM_DEFAULT_A_SCHEMA)
        .noDefault().endRecord();

    Schema readerSchema = SchemaBuilder.record("Record1").fields().name("field1").type(ENUM_AB_ENUM_DEFAULT_A_SCHEMA)
        .withDefault("B").endRecord();

    GenericRecord datum = new GenericData.Record(writerSchema);
    datum.put("field1", new GenericData.EnumSymbol(writerSchema, "C"));
    GenericRecord decodedDatum = serializeWithWriterThenDeserializeWithReader(writerSchema, datum, readerSchema);
    // The A is the Enum default, which is assigned since C is not in [A,B].
    assertEquals("A", decodedDatum.get("field1").toString());
  }

  @Test
  public void testFieldDefaultNotAppliedForUnknownSymbol() throws Exception {
    expectedException.expect(AvroTypeException.class);
    expectedException.expectMessage("No match for C");

    Schema writerSchema = SchemaBuilder.record("Record1").fields().name("field1").type(ENUM1_ABC_SCHEMA).noDefault()
        .endRecord();
    Schema readerSchema = SchemaBuilder.record("Record1").fields().name("field1").type(ENUM1_AB_SCHEMA).withDefault("A")
        .endRecord();

    GenericRecord datum = new GenericData.Record(writerSchema);
    datum.put("field1", new GenericData.EnumSymbol(writerSchema, "C"));
    serializeWithWriterThenDeserializeWithReader(writerSchema, datum, readerSchema);
  }

  private GenericRecord serializeWithWriterThenDeserializeWithReader(Schema writerSchema, GenericRecord datum,
      Schema readerSchema) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
    DatumWriter<Object> datumWriter = new GenericDatumWriter<>(writerSchema);
    datumWriter.write(datum, encoder);
    encoder.flush();

    byte[] bytes = baos.toByteArray();
    Decoder decoder = DecoderFactory.get().resolvingDecoder(writerSchema, readerSchema,
        DecoderFactory.get().binaryDecoder(bytes, null));
    DatumReader<Object> datumReader = new GenericDatumReader<>(readerSchema);
    return (GenericRecord) datumReader.read(null, decoder);
  }

}
