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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestGenericLogicalTypes {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  public static final GenericData GENERIC = new GenericData();

  @BeforeClass
  public static void addLogicalTypes() {
    GENERIC.addLogicalTypeConversion(new Conversions.DecimalConversion());
    GENERIC.addLogicalTypeConversion(new Conversions.UUIDConversion());
    GENERIC.addLogicalTypeConversion(new TimeConversions.LocalTimestampMicrosConversion());
    GENERIC.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
  }

  @Test
  public void testReadUUID() throws IOException {
    Schema uuidSchema = Schema.create(Schema.Type.STRING);
    LogicalTypes.uuid().addToSchema(uuidSchema);

    UUID u1 = UUID.randomUUID();
    UUID u2 = UUID.randomUUID();
    List<UUID> expected = Arrays.asList(u1, u2);

    File test = write(Schema.create(Schema.Type.STRING), u1.toString(), u2.toString());
    Assert.assertEquals("Should convert Strings to UUIDs", expected, read(GENERIC.createDatumReader(uuidSchema), test));
  }

  @Test
  public void testWriteUUID() throws IOException {
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    stringSchema.addProp(GenericData.STRING_PROP, "String");
    Schema uuidSchema = Schema.create(Schema.Type.STRING);
    LogicalTypes.uuid().addToSchema(uuidSchema);

    UUID u1 = UUID.randomUUID();
    UUID u2 = UUID.randomUUID();
    List<String> expected = Arrays.asList(u1.toString(), u2.toString());

    File test = write(GENERIC, uuidSchema, u1, u2);
    Assert.assertEquals("Should read UUIDs as Strings", expected,
        read(GenericData.get().createDatumReader(stringSchema), test));
  }

  @Test
  public void testWriteNullableUUID() throws IOException {
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    stringSchema.addProp(GenericData.STRING_PROP, "String");
    Schema nullableStringSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), stringSchema);

    Schema uuidSchema = Schema.create(Schema.Type.STRING);
    LogicalTypes.uuid().addToSchema(uuidSchema);
    Schema nullableUuidSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), uuidSchema);

    UUID u1 = UUID.randomUUID();
    UUID u2 = UUID.randomUUID();
    List<String> expected = Arrays.asList(u1.toString(), u2.toString());

    File test = write(GENERIC, nullableUuidSchema, u1, u2);
    Assert.assertEquals("Should read UUIDs as Strings", expected,
        read(GenericData.get().createDatumReader(nullableStringSchema), test));
  }

  @Test
  public void testReadDecimalFixed() throws IOException {
    LogicalType decimal = LogicalTypes.decimal(9, 2);
    Schema fixedSchema = Schema.createFixed("aFixed", null, null, 4);
    Schema decimalSchema = decimal.addToSchema(Schema.createFixed("aFixed", null, null, 4));

    BigDecimal d1 = new BigDecimal("-34.34");
    BigDecimal d2 = new BigDecimal("117230.00");
    List<BigDecimal> expected = Arrays.asList(d1, d2);

    Conversion<BigDecimal> conversion = new Conversions.DecimalConversion();

    // use the conversion directly instead of relying on the write side
    GenericFixed d1fixed = conversion.toFixed(d1, fixedSchema, decimal);
    GenericFixed d2fixed = conversion.toFixed(d2, fixedSchema, decimal);

    File test = write(fixedSchema, d1fixed, d2fixed);
    Assert.assertEquals("Should convert fixed to BigDecimals", expected,
        read(GENERIC.createDatumReader(decimalSchema), test));
  }

  @Test
  public void testWriteDecimalFixed() throws IOException {
    LogicalType decimal = LogicalTypes.decimal(9, 2);
    Schema fixedSchema = Schema.createFixed("aFixed", null, null, 4);
    Schema decimalSchema = decimal.addToSchema(Schema.createFixed("aFixed", null, null, 4));

    BigDecimal d1 = new BigDecimal("-34.34");
    BigDecimal d2 = new BigDecimal("117230.00");

    Conversion<BigDecimal> conversion = new Conversions.DecimalConversion();

    GenericFixed d1fixed = conversion.toFixed(d1, fixedSchema, decimal);
    GenericFixed d2fixed = conversion.toFixed(d2, fixedSchema, decimal);
    List<GenericFixed> expected = Arrays.asList(d1fixed, d2fixed);

    File test = write(GENERIC, decimalSchema, d1, d2);
    Assert.assertEquals("Should read BigDecimals as fixed", expected,
        read(GenericData.get().createDatumReader(fixedSchema), test));
  }

  @Test
  public void testDecimalToFromBytes() throws IOException {
    LogicalType decimal = LogicalTypes.decimal(9, 2);
    Schema bytesSchema = Schema.create(Schema.Type.BYTES);

    // Check that the round trip to and from bytes
    BigDecimal d1 = new BigDecimal("-34.34");
    BigDecimal d2 = new BigDecimal("117230.00");

    Conversion<BigDecimal> conversion = new Conversions.DecimalConversion();

    ByteBuffer d1bytes = conversion.toBytes(d1, bytesSchema, decimal);
    ByteBuffer d2bytes = conversion.toBytes(d2, bytesSchema, decimal);

    assertThat(conversion.fromBytes(d1bytes, bytesSchema, decimal), is(d1));
    assertThat(conversion.fromBytes(d2bytes, bytesSchema, decimal), is(d2));

    assertThat("Ensure ByteBuffer not consumed by conversion", conversion.fromBytes(d1bytes, bytesSchema, decimal),
        is(d1));
  }

  @Test
  public void testDecimalToFromFixed() throws IOException {
    LogicalType decimal = LogicalTypes.decimal(9, 2);
    Schema fixedSchema = Schema.createFixed("aFixed", null, null, 4);

    // Check that the round trip to and from fixed data.
    BigDecimal d1 = new BigDecimal("-34.34");
    BigDecimal d2 = new BigDecimal("117230.00");

    Conversion<BigDecimal> conversion = new Conversions.DecimalConversion();

    GenericFixed d1fixed = conversion.toFixed(d1, fixedSchema, decimal);
    GenericFixed d2fixed = conversion.toFixed(d2, fixedSchema, decimal);
    assertThat(conversion.fromFixed(d1fixed, fixedSchema, decimal), is(d1));
    assertThat(conversion.fromFixed(d2fixed, fixedSchema, decimal), is(d2));
  }

  @Test
  public void testReadDecimalBytes() throws IOException {
    LogicalType decimal = LogicalTypes.decimal(9, 2);
    Schema bytesSchema = Schema.create(Schema.Type.BYTES);
    Schema decimalSchema = decimal.addToSchema(Schema.create(Schema.Type.BYTES));

    BigDecimal d1 = new BigDecimal("-34.34");
    BigDecimal d2 = new BigDecimal("117230.00");
    List<BigDecimal> expected = Arrays.asList(d1, d2);

    Conversion<BigDecimal> conversion = new Conversions.DecimalConversion();

    // use the conversion directly instead of relying on the write side
    ByteBuffer d1bytes = conversion.toBytes(d1, bytesSchema, decimal);
    ByteBuffer d2bytes = conversion.toBytes(d2, bytesSchema, decimal);

    File test = write(bytesSchema, d1bytes, d2bytes);
    Assert.assertEquals("Should convert bytes to BigDecimals", expected,
        read(GENERIC.createDatumReader(decimalSchema), test));
  }

  @Test
  public void testWriteDecimalBytes() throws IOException {
    LogicalType decimal = LogicalTypes.decimal(9, 2);
    Schema bytesSchema = Schema.create(Schema.Type.BYTES);
    Schema decimalSchema = decimal.addToSchema(Schema.create(Schema.Type.BYTES));

    BigDecimal d1 = new BigDecimal("-34.34");
    BigDecimal d2 = new BigDecimal("117230.00");

    Conversion<BigDecimal> conversion = new Conversions.DecimalConversion();

    // use the conversion directly instead of relying on the write side
    ByteBuffer d1bytes = conversion.toBytes(d1, bytesSchema, decimal);
    ByteBuffer d2bytes = conversion.toBytes(d2, bytesSchema, decimal);
    List<ByteBuffer> expected = Arrays.asList(d1bytes, d2bytes);

    File test = write(GENERIC, decimalSchema, d1bytes, d2bytes);
    Assert.assertEquals("Should read BigDecimals as bytes", expected,
        read(GenericData.get().createDatumReader(bytesSchema), test));
  }

  private <D> List<D> read(DatumReader<D> reader, File file) throws IOException {
    List<D> data = new ArrayList<>();

    try (FileReader<D> fileReader = new DataFileReader<>(file, reader)) {
      for (D datum : fileReader) {
        data.add(datum);
      }
    }

    return data;
  }

  private <D> File write(Schema schema, D... data) throws IOException {
    return write(GenericData.get(), schema, data);
  }

  @SuppressWarnings("unchecked")
  private <D> File write(GenericData model, Schema schema, D... data) throws IOException {
    File file = temp.newFile();
    DatumWriter<D> writer = model.createDatumWriter(schema);

    try (DataFileWriter<D> fileWriter = new DataFileWriter<>(writer)) {
      fileWriter.create(schema, file);
      for (D datum : data) {
        fileWriter.append(datum);
      }
    }

    return file;
  }

  @Test
  public void testCopyUuid() {
    testCopy(LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING)), UUID.randomUUID(), GENERIC);
  }

  @Test
  public void testCopyUuidRaw() {
    testCopy(LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING)), UUID.randomUUID().toString(), // use
                                                                                                               // raw
                                                                                                               // type
        GenericData.get()); // with no conversions
  }

  @Test
  public void testCopyDecimal() {
    testCopy(LogicalTypes.decimal(9, 2).addToSchema(Schema.create(Schema.Type.BYTES)), new BigDecimal("-34.34"),
        GENERIC);
  }

  @Test
  public void testCopyDecimalRaw() {
    testCopy(LogicalTypes.decimal(9, 2).addToSchema(Schema.create(Schema.Type.BYTES)),
        ByteBuffer.wrap(new BigDecimal("-34.34").unscaledValue().toByteArray()), GenericData.get()); // no conversions
  }

  private void testCopy(Schema schema, Object value, GenericData model) {
    // test direct copy of instance
    checkCopy(value, model.deepCopy(schema, value), false);

    // test nested in a record
    Schema recordSchema = Schema.createRecord("X", "", "test", false);
    List<Schema.Field> fields = new ArrayList<>();
    fields.add(new Schema.Field("x", schema, "", null));
    recordSchema.setFields(fields);

    GenericRecordBuilder builder = new GenericRecordBuilder(recordSchema);
    builder.set("x", value);
    GenericData.Record record = builder.build();
    checkCopy(record, model.deepCopy(recordSchema, record), true);

    // test nested in array
    Schema arraySchema = Schema.createArray(schema);
    ArrayList array = new ArrayList(Collections.singletonList(value));
    checkCopy(array, model.deepCopy(arraySchema, array), true);

    // test record nested in array
    Schema recordArraySchema = Schema.createArray(recordSchema);
    ArrayList recordArray = new ArrayList(Collections.singletonList(record));
    checkCopy(recordArray, model.deepCopy(recordArraySchema, recordArray), true);
  }

  private void checkCopy(Object original, Object copy, boolean notSame) {
    if (notSame)
      Assert.assertNotSame(original, copy);
    Assert.assertEquals(original, copy);
  }

  @Test
  public void testReadLocalTimestampMillis() throws IOException {
    LogicalType timestamp = LogicalTypes.localTimestampMillis();
    Schema longSchema = Schema.create(Schema.Type.LONG);
    Schema timestampSchema = timestamp.addToSchema(Schema.create(Schema.Type.LONG));

    LocalDateTime i1 = LocalDateTime.of(1986, 06, 26, 12, 07, 11, 42000000);
    LocalDateTime i2 = LocalDateTime.ofInstant(Instant.ofEpochMilli(0), ZoneOffset.UTC);
    List<LocalDateTime> expected = Arrays.asList(i1, i2);

    Conversion<LocalDateTime> conversion = new TimeConversions.LocalTimestampMillisConversion();

    // use the conversion directly instead of relying on the write side
    Long i1long = conversion.toLong(i1, longSchema, timestamp);
    Long i2long = 0L;

    File test = write(longSchema, i1long, i2long);
    Assert.assertEquals("Should convert long to LocalDateTime", expected,
        read(GENERIC.createDatumReader(timestampSchema), test));
  }

  @Test
  public void testWriteLocalTimestampMillis() throws IOException {
    LogicalType timestamp = LogicalTypes.localTimestampMillis();
    Schema longSchema = Schema.create(Schema.Type.LONG);
    Schema timestampSchema = timestamp.addToSchema(Schema.create(Schema.Type.LONG));

    LocalDateTime i1 = LocalDateTime.of(1986, 06, 26, 12, 07, 11, 42000000);
    LocalDateTime i2 = LocalDateTime.ofInstant(Instant.ofEpochMilli(0), ZoneOffset.UTC);

    Conversion<LocalDateTime> conversion = new TimeConversions.LocalTimestampMillisConversion();

    Long d1long = conversion.toLong(i1, longSchema, timestamp);
    Long d2long = 0L;
    List<Long> expected = Arrays.asList(d1long, d2long);

    File test = write(GENERIC, timestampSchema, i1, i2);
    Assert.assertEquals("Should read LocalDateTime as longs", expected,
        read(GenericData.get().createDatumReader(timestampSchema), test));
  }

  @Test
  public void testReadLocalTimestampMicros() throws IOException {
    LogicalType timestamp = LogicalTypes.localTimestampMicros();
    Schema longSchema = Schema.create(Schema.Type.LONG);
    Schema timestampSchema = timestamp.addToSchema(Schema.create(Schema.Type.LONG));

    LocalDateTime i1 = LocalDateTime.of(1986, 06, 26, 12, 07, 11, 420000);
    LocalDateTime i2 = LocalDateTime.ofInstant(Instant.ofEpochSecond(0, 4000), ZoneOffset.UTC);
    List<LocalDateTime> expected = Arrays.asList(i1, i2);

    Conversion<LocalDateTime> conversion = new TimeConversions.LocalTimestampMicrosConversion();

    // use the conversion directly instead of relying on the write side
    Long i1long = conversion.toLong(i1, longSchema, timestamp);
    Long i2long = conversion.toLong(i2, longSchema, timestamp);

    File test = write(longSchema, i1long, i2long);
    Assert.assertEquals("Should convert long to LocalDateTime", expected,
        read(GENERIC.createDatumReader(timestampSchema), test));
  }

  @Test
  public void testWriteLocalTimestampMicros() throws IOException {
    LogicalType timestamp = LogicalTypes.localTimestampMicros();
    Schema longSchema = Schema.create(Schema.Type.LONG);
    Schema timestampSchema = timestamp.addToSchema(Schema.create(Schema.Type.LONG));

    LocalDateTime i1 = LocalDateTime.of(1986, 06, 26, 12, 07, 11, 420000);
    LocalDateTime i2 = LocalDateTime.ofInstant(Instant.ofEpochSecond(0, 4000), ZoneOffset.UTC);

    Conversion<LocalDateTime> conversion = new TimeConversions.LocalTimestampMicrosConversion();

    Long d1long = conversion.toLong(i1, longSchema, timestamp);
    Long d2long = conversion.toLong(i2, longSchema, timestamp);
    List<Long> expected = Arrays.asList(d1long, d2long);

    File test = write(GENERIC, timestampSchema, i1, i2);
    Assert.assertEquals("Should read LocalDateTime as longs", expected,
        read(GenericData.get().createDatumReader(timestampSchema), test));
  }
}
