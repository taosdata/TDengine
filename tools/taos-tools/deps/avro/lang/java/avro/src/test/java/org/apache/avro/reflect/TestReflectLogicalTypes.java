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

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificData;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests various logical types * string => UUID * fixed and bytes => Decimal *
 * record => Pair
 */
public class TestReflectLogicalTypes {
  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  public static final ReflectData REFLECT = new ReflectData();

  @BeforeClass
  public static void addUUID() {
    REFLECT.addLogicalTypeConversion(new Conversions.UUIDConversion());
    REFLECT.addLogicalTypeConversion(new Conversions.DecimalConversion());
    REFLECT.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
  }

  @Test
  public void testReflectedSchema() {
    Schema expected = SchemaBuilder.record(RecordWithUUIDList.class.getName()).fields().name("uuids").type().array()
        .items().stringType().noDefault().endRecord();
    expected.getField("uuids").schema().addProp(SpecificData.CLASS_PROP, List.class.getName());
    LogicalTypes.uuid().addToSchema(expected.getField("uuids").schema().getElementType());

    Schema actual = REFLECT.getSchema(RecordWithUUIDList.class);

    Assert.assertEquals("Should use the UUID logical type", expected, actual);
  }

  // this can be static because the schema only comes from reflection
  public static class DecimalRecordBytes {
    // scale is required and will not be set by the conversion
    @AvroSchema("{" + "\"type\": \"bytes\"," + "\"logicalType\": \"decimal\"," + "\"precision\": 9," + "\"scale\": 2"
        + "}")
    private BigDecimal decimal;

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }

      if (other == null || getClass() != other.getClass()) {
        return false;
      }

      DecimalRecordBytes that = (DecimalRecordBytes) other;
      if (decimal == null) {
        return (that.decimal == null);
      }

      return decimal.equals(that.decimal);
    }

    @Override
    public int hashCode() {
      return decimal != null ? decimal.hashCode() : 0;
    }
  }

  @Test
  public void testDecimalBytes() throws IOException {
    Schema schema = REFLECT.getSchema(DecimalRecordBytes.class);
    Assert.assertEquals("Should have the correct record name", "org.apache.avro.reflect.TestReflectLogicalTypes",
        schema.getNamespace());
    Assert.assertEquals("Should have the correct record name", "DecimalRecordBytes", schema.getName());
    Assert.assertEquals("Should have the correct logical type", LogicalTypes.decimal(9, 2),
        LogicalTypes.fromSchema(schema.getField("decimal").schema()));

    DecimalRecordBytes record = new DecimalRecordBytes();
    record.decimal = new BigDecimal("3.14");

    File test = write(REFLECT, schema, record);
    Assert.assertEquals("Should match the decimal after round trip", Collections.singletonList(record),
        read(REFLECT.createDatumReader(schema), test));
  }

  // this can be static because the schema only comes from reflection
  public static class DecimalRecordFixed {
    // scale is required and will not be set by the conversion
    @AvroSchema("{" + "\"name\": \"decimal_9\"," + "\"type\": \"fixed\"," + "\"size\": 4,"
        + "\"logicalType\": \"decimal\"," + "\"precision\": 9," + "\"scale\": 2" + "}")
    private BigDecimal decimal;

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }

      if (other == null || getClass() != other.getClass()) {
        return false;
      }

      DecimalRecordFixed that = (DecimalRecordFixed) other;
      if (decimal == null) {
        return (that.decimal == null);
      }

      return decimal.equals(that.decimal);
    }

    @Override
    public int hashCode() {
      return decimal != null ? decimal.hashCode() : 0;
    }
  }

  @Test
  public void testDecimalFixed() throws IOException {
    Schema schema = REFLECT.getSchema(DecimalRecordFixed.class);
    Assert.assertEquals("Should have the correct record name", "org.apache.avro.reflect.TestReflectLogicalTypes",
        schema.getNamespace());
    Assert.assertEquals("Should have the correct record name", "DecimalRecordFixed", schema.getName());
    Assert.assertEquals("Should have the correct logical type", LogicalTypes.decimal(9, 2),
        LogicalTypes.fromSchema(schema.getField("decimal").schema()));

    DecimalRecordFixed record = new DecimalRecordFixed();
    record.decimal = new BigDecimal("3.14");

    File test = write(REFLECT, schema, record);
    Assert.assertEquals("Should match the decimal after round trip", Collections.singletonList(record),
        read(REFLECT.createDatumReader(schema), test));
  }

  public static class Pair<X, Y> {
    private final X first;
    private final Y second;

    private Pair(X first, Y second) {
      this.first = first;
      this.second = second;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }

      if (other == null || getClass() != other.getClass()) {
        return false;
      }

      Pair<?, ?> that = (Pair<?, ?>) other;
      if (first == null) {
        if (that.first != null) {
          return false;
        }
      } else if (first.equals(that.first)) {
        return false;
      }

      if (second == null) {
        return that.second == null;
      } else
        return !second.equals(that.second);

    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(new Object[] { first, second });
    }

    public static <X, Y> Pair<X, Y> of(X first, Y second) {
      return new Pair<>(first, second);
    }
  }

  public static class PairRecord {
    @AvroSchema("{" + "\"name\": \"Pair\"," + "\"type\": \"record\"," + "\"fields\": ["
        + "    {\"name\": \"x\", \"type\": \"long\"}," + "    {\"name\": \"y\", \"type\": \"long\"}" + "  ],"
        + "\"logicalType\": \"pair\"" + "}")
    Pair<Long, Long> pair;
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testPairRecord() throws IOException {
    ReflectData model = new ReflectData();
    model.addLogicalTypeConversion(new Conversion<Pair>() {
      @Override
      public Class<Pair> getConvertedType() {
        return Pair.class;
      }

      @Override
      public String getLogicalTypeName() {
        return "pair";
      }

      @Override
      public Pair fromRecord(IndexedRecord value, Schema schema, LogicalType type) {
        return Pair.of(value.get(0), value.get(1));
      }

      @Override
      public IndexedRecord toRecord(Pair value, Schema schema, LogicalType type) {
        GenericData.Record record = new GenericData.Record(schema);
        record.put(0, value.first);
        record.put(1, value.second);
        return record;
      }
    });

    LogicalTypes.register("pair", new LogicalTypes.LogicalTypeFactory() {
      private final LogicalType PAIR = new LogicalType("pair");

      @Override
      public LogicalType fromSchema(Schema schema) {
        return PAIR;
      }

      @Override
      public String getTypeName() {
        return "pair";
      }
    });

    Schema schema = model.getSchema(PairRecord.class);
    Assert.assertEquals("Should have the correct record name", "org.apache.avro.reflect.TestReflectLogicalTypes",
        schema.getNamespace());
    Assert.assertEquals("Should have the correct record name", "PairRecord", schema.getName());
    Assert.assertEquals("Should have the correct logical type", "pair",
        LogicalTypes.fromSchema(schema.getField("pair").schema()).getName());

    PairRecord record = new PairRecord();
    record.pair = Pair.of(34L, 35L);
    List<PairRecord> expected = new ArrayList<>();
    expected.add(record);

    File test = write(model, schema, record);
    Pair<Long, Long> actual = ((PairRecord) TestReflectLogicalTypes
        .<PairRecord>read(model.createDatumReader(schema), test).get(0)).pair;
    Assert.assertEquals("Data should match after serialization round-trip", 34L, (long) actual.first);
    Assert.assertEquals("Data should match after serialization round-trip", 35L, (long) actual.second);
  }

  @Test
  public void testReadUUID() throws IOException {
    Schema uuidSchema = SchemaBuilder.record(RecordWithUUID.class.getName()).fields().requiredString("uuid")
        .endRecord();
    LogicalTypes.uuid().addToSchema(uuidSchema.getField("uuid").schema());

    UUID u1 = UUID.randomUUID();
    UUID u2 = UUID.randomUUID();

    RecordWithStringUUID r1 = new RecordWithStringUUID();
    r1.uuid = u1.toString();
    RecordWithStringUUID r2 = new RecordWithStringUUID();
    r2.uuid = u2.toString();

    List<RecordWithUUID> expected = Arrays.asList(new RecordWithUUID(), new RecordWithUUID());
    expected.get(0).uuid = u1;
    expected.get(1).uuid = u2;

    File test = write(ReflectData.get().getSchema(RecordWithStringUUID.class), r1, r2);

    Assert.assertEquals("Should convert Strings to UUIDs", expected, read(REFLECT.createDatumReader(uuidSchema), test));

    // verify that the field's type overrides the logical type
    Schema uuidStringSchema = SchemaBuilder.record(RecordWithStringUUID.class.getName()).fields().requiredString("uuid")
        .endRecord();
    LogicalTypes.uuid().addToSchema(uuidStringSchema.getField("uuid").schema());

    Assert.assertEquals("Should not convert to UUID if accessor is String", Arrays.asList(r1, r2),
        read(REFLECT.createDatumReader(uuidStringSchema), test));
  }

  @Test
  public void testWriteUUID() throws IOException {
    Schema uuidSchema = SchemaBuilder.record(RecordWithUUID.class.getName()).fields().requiredString("uuid")
        .endRecord();
    LogicalTypes.uuid().addToSchema(uuidSchema.getField("uuid").schema());

    UUID u1 = UUID.randomUUID();
    UUID u2 = UUID.randomUUID();

    RecordWithUUID r1 = new RecordWithUUID();
    r1.uuid = u1;
    RecordWithUUID r2 = new RecordWithUUID();
    r2.uuid = u2;

    List<RecordWithStringUUID> expected = Arrays.asList(new RecordWithStringUUID(), new RecordWithStringUUID());
    expected.get(0).uuid = u1.toString();
    expected.get(1).uuid = u2.toString();

    File test = write(REFLECT, uuidSchema, r1, r2);

    // verify that the field's type overrides the logical type
    Schema uuidStringSchema = SchemaBuilder.record(RecordWithStringUUID.class.getName()).fields().requiredString("uuid")
        .endRecord();

    Assert.assertEquals("Should read uuid as String without UUID conversion", expected,
        read(REFLECT.createDatumReader(uuidStringSchema), test));

    LogicalTypes.uuid().addToSchema(uuidStringSchema.getField("uuid").schema());
    Assert.assertEquals("Should read uuid as String without UUID logical type", expected,
        read(ReflectData.get().createDatumReader(uuidStringSchema), test));
  }

  @Test
  public void testWriteNullableUUID() throws IOException {
    Schema nullableUuidSchema = SchemaBuilder.record(RecordWithUUID.class.getName()).fields().optionalString("uuid")
        .endRecord();
    LogicalTypes.uuid().addToSchema(nullableUuidSchema.getField("uuid").schema().getTypes().get(1));

    UUID u1 = UUID.randomUUID();
    UUID u2 = UUID.randomUUID();

    RecordWithUUID r1 = new RecordWithUUID();
    r1.uuid = u1;
    RecordWithUUID r2 = new RecordWithUUID();
    r2.uuid = u2;

    List<RecordWithStringUUID> expected = Arrays.asList(new RecordWithStringUUID(), new RecordWithStringUUID());
    expected.get(0).uuid = u1.toString();
    expected.get(1).uuid = u2.toString();

    File test = write(REFLECT, nullableUuidSchema, r1, r2);

    // verify that the field's type overrides the logical type
    Schema nullableUuidStringSchema = SchemaBuilder.record(RecordWithStringUUID.class.getName()).fields()
        .optionalString("uuid").endRecord();

    Assert.assertEquals("Should read uuid as String without UUID conversion", expected,
        read(ReflectData.get().createDatumReader(nullableUuidStringSchema), test));
  }

  @Test
  public void testWriteNullableUUIDReadRequiredString() throws IOException {
    Schema nullableUuidSchema = SchemaBuilder.record(RecordWithUUID.class.getName()).fields().optionalString("uuid")
        .endRecord();
    LogicalTypes.uuid().addToSchema(nullableUuidSchema.getField("uuid").schema().getTypes().get(1));

    UUID u1 = UUID.randomUUID();
    UUID u2 = UUID.randomUUID();

    RecordWithUUID r1 = new RecordWithUUID();
    r1.uuid = u1;
    RecordWithUUID r2 = new RecordWithUUID();
    r2.uuid = u2;

    List<RecordWithStringUUID> expected = Arrays.asList(new RecordWithStringUUID(), new RecordWithStringUUID());
    expected.get(0).uuid = u1.toString();
    expected.get(1).uuid = u2.toString();

    File test = write(REFLECT, nullableUuidSchema, r1, r2);

    // verify that the field's type overrides the logical type
    Schema uuidStringSchema = SchemaBuilder.record(RecordWithStringUUID.class.getName()).fields().requiredString("uuid")
        .endRecord();

    Assert.assertEquals("Should read uuid as String without UUID conversion", expected,
        read(REFLECT.createDatumReader(uuidStringSchema), test));
  }

  @Test
  public void testReadUUIDMissingLogicalTypeUnsafe() throws IOException {
    String unsafeValue = System.getProperty("avro.disable.unsafe");
    try {
      // only one FieldAccess can be set per JVM
      System.clearProperty("avro.disable.unsafe");
      Assume.assumeTrue(ReflectionUtil.getFieldAccess() instanceof FieldAccessUnsafe);

      Schema uuidSchema = SchemaBuilder.record(RecordWithUUID.class.getName()).fields().requiredString("uuid")
          .endRecord();
      LogicalTypes.uuid().addToSchema(uuidSchema.getField("uuid").schema());

      UUID u1 = UUID.randomUUID();

      RecordWithStringUUID r1 = new RecordWithStringUUID();
      r1.uuid = u1.toString();

      File test = write(ReflectData.get().getSchema(RecordWithStringUUID.class), r1);

      RecordWithUUID datum = (RecordWithUUID) read(ReflectData.get().createDatumReader(uuidSchema), test).get(0);
      Object uuid = datum.uuid;
      Assert.assertTrue("UUID should be a String (unsafe)", uuid instanceof String);
    } finally {
      if (unsafeValue != null) {
        System.setProperty("avro.disable.unsafe", unsafeValue);
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testReadUUIDMissingLogicalTypeReflect() throws IOException {
    String unsafeValue = System.getProperty("avro.disable.unsafe");
    try {
      // only one FieldAccess can be set per JVM
      System.setProperty("avro.disable.unsafe", "true");
      Assume.assumeTrue(ReflectionUtil.getFieldAccess() instanceof FieldAccessReflect);

      Schema uuidSchema = SchemaBuilder.record(RecordWithUUID.class.getName()).fields().requiredString("uuid")
          .endRecord();
      LogicalTypes.uuid().addToSchema(uuidSchema.getField("uuid").schema());

      UUID u1 = UUID.randomUUID();

      RecordWithStringUUID r1 = new RecordWithStringUUID();
      r1.uuid = u1.toString();

      File test = write(ReflectData.get().getSchema(RecordWithStringUUID.class), r1);

      read(ReflectData.get().createDatumReader(uuidSchema), test).get(0);
    } finally {
      if (unsafeValue != null) {
        System.setProperty("avro.disable.unsafe", unsafeValue);
      }
    }
  }

  @Test(expected = DataFileWriter.AppendWriteException.class)
  public void testWriteUUIDMissingLogicalType() throws IOException {
    Schema uuidSchema = SchemaBuilder.record(RecordWithUUID.class.getName()).fields().requiredString("uuid")
        .endRecord();
    LogicalTypes.uuid().addToSchema(uuidSchema.getField("uuid").schema());

    UUID u1 = UUID.randomUUID();
    UUID u2 = UUID.randomUUID();

    RecordWithUUID r1 = new RecordWithUUID();
    r1.uuid = u1;
    RecordWithUUID r2 = new RecordWithUUID();
    r2.uuid = u2;

    // write without using REFLECT, which has the logical type
    File test = write(uuidSchema, r1, r2);

    // verify that the field's type overrides the logical type
    Schema uuidStringSchema = SchemaBuilder.record(RecordWithStringUUID.class.getName()).fields().requiredString("uuid")
        .endRecord();

    // this fails with an AppendWriteException wrapping ClassCastException
    // because the UUID isn't converted to a CharSequence expected internally
    read(ReflectData.get().createDatumReader(uuidStringSchema), test);
  }

  @Test
  public void testReadUUIDGenericRecord() throws IOException {
    Schema uuidSchema = SchemaBuilder.record("RecordWithUUID").fields().requiredString("uuid").endRecord();
    LogicalTypes.uuid().addToSchema(uuidSchema.getField("uuid").schema());

    UUID u1 = UUID.randomUUID();
    UUID u2 = UUID.randomUUID();

    RecordWithStringUUID r1 = new RecordWithStringUUID();
    r1.uuid = u1.toString();
    RecordWithStringUUID r2 = new RecordWithStringUUID();
    r2.uuid = u2.toString();

    List<GenericData.Record> expected = Arrays.asList(new GenericData.Record(uuidSchema),
        new GenericData.Record(uuidSchema));
    expected.get(0).put("uuid", u1);
    expected.get(1).put("uuid", u2);

    File test = write(ReflectData.get().getSchema(RecordWithStringUUID.class), r1, r2);

    Assert.assertEquals("Should convert Strings to UUIDs", expected, read(REFLECT.createDatumReader(uuidSchema), test));

    // verify that the field's type overrides the logical type
    Schema uuidStringSchema = SchemaBuilder.record(RecordWithStringUUID.class.getName()).fields().requiredString("uuid")
        .endRecord();
    LogicalTypes.uuid().addToSchema(uuidSchema.getField("uuid").schema());

    Assert.assertEquals("Should not convert to UUID if accessor is String", Arrays.asList(r1, r2),
        read(REFLECT.createDatumReader(uuidStringSchema), test));
  }

  @Test
  public void testReadUUIDArray() throws IOException {
    Schema uuidArraySchema = SchemaBuilder.record(RecordWithUUIDArray.class.getName()).fields().name("uuids").type()
        .array().items().stringType().noDefault().endRecord();
    LogicalTypes.uuid().addToSchema(uuidArraySchema.getField("uuids").schema().getElementType());

    UUID u1 = UUID.randomUUID();
    UUID u2 = UUID.randomUUID();

    GenericRecord r = new GenericData.Record(uuidArraySchema);
    r.put("uuids", Arrays.asList(u1.toString(), u2.toString()));

    RecordWithUUIDArray expected = new RecordWithUUIDArray();
    expected.uuids = new UUID[] { u1, u2 };

    File test = write(uuidArraySchema, r);

    Assert.assertEquals("Should convert Strings to UUIDs", expected,
        read(REFLECT.createDatumReader(uuidArraySchema), test).get(0));
  }

  @Test
  public void testWriteUUIDArray() throws IOException {
    Schema uuidArraySchema = SchemaBuilder.record(RecordWithUUIDArray.class.getName()).fields().name("uuids").type()
        .array().items().stringType().noDefault().endRecord();
    LogicalTypes.uuid().addToSchema(uuidArraySchema.getField("uuids").schema().getElementType());

    Schema stringArraySchema = SchemaBuilder.record("RecordWithUUIDArray").fields().name("uuids").type().array().items()
        .stringType().noDefault().endRecord();
    stringArraySchema.getField("uuids").schema().addProp(SpecificData.CLASS_PROP, List.class.getName());

    UUID u1 = UUID.randomUUID();
    UUID u2 = UUID.randomUUID();

    GenericRecord expected = new GenericData.Record(stringArraySchema);
    List<String> uuids = new ArrayList<>();
    uuids.add(u1.toString());
    uuids.add(u2.toString());
    expected.put("uuids", uuids);

    RecordWithUUIDArray r = new RecordWithUUIDArray();
    r.uuids = new UUID[] { u1, u2 };

    File test = write(REFLECT, uuidArraySchema, r);

    Assert.assertEquals("Should read UUIDs as Strings", expected,
        read(ReflectData.get().createDatumReader(stringArraySchema), test).get(0));
  }

  @Test
  public void testReadUUIDList() throws IOException {
    Schema uuidListSchema = SchemaBuilder.record(RecordWithUUIDList.class.getName()).fields().name("uuids").type()
        .array().items().stringType().noDefault().endRecord();
    uuidListSchema.getField("uuids").schema().addProp(SpecificData.CLASS_PROP, List.class.getName());
    LogicalTypes.uuid().addToSchema(uuidListSchema.getField("uuids").schema().getElementType());

    UUID u1 = UUID.randomUUID();
    UUID u2 = UUID.randomUUID();

    GenericRecord r = new GenericData.Record(uuidListSchema);
    r.put("uuids", Arrays.asList(u1.toString(), u2.toString()));

    RecordWithUUIDList expected = new RecordWithUUIDList();
    expected.uuids = Arrays.asList(u1, u2);

    File test = write(uuidListSchema, r);

    Assert.assertEquals("Should convert Strings to UUIDs", expected,
        read(REFLECT.createDatumReader(uuidListSchema), test).get(0));
  }

  @Test
  public void testWriteUUIDList() throws IOException {
    Schema uuidListSchema = SchemaBuilder.record(RecordWithUUIDList.class.getName()).fields().name("uuids").type()
        .array().items().stringType().noDefault().endRecord();
    uuidListSchema.getField("uuids").schema().addProp(SpecificData.CLASS_PROP, List.class.getName());
    LogicalTypes.uuid().addToSchema(uuidListSchema.getField("uuids").schema().getElementType());

    Schema stringArraySchema = SchemaBuilder.record("RecordWithUUIDArray").fields().name("uuids").type().array().items()
        .stringType().noDefault().endRecord();
    stringArraySchema.getField("uuids").schema().addProp(SpecificData.CLASS_PROP, List.class.getName());

    UUID u1 = UUID.randomUUID();
    UUID u2 = UUID.randomUUID();

    GenericRecord expected = new GenericData.Record(stringArraySchema);
    expected.put("uuids", Arrays.asList(u1.toString(), u2.toString()));

    RecordWithUUIDList r = new RecordWithUUIDList();
    r.uuids = Arrays.asList(u1, u2);

    File test = write(REFLECT, uuidListSchema, r);

    Assert.assertEquals("Should read UUIDs as Strings", expected,
        read(REFLECT.createDatumReader(stringArraySchema), test).get(0));
  }

  @Test
  public void testReflectedSchemaLocalDateTime() {
    Schema actual = REFLECT.getSchema(RecordWithTimestamps.class);

    Assert.assertEquals("Should have the correct record name", "org.apache.avro.reflect", actual.getNamespace());
    Assert.assertEquals("Should have the correct record name", "RecordWithTimestamps", actual.getName());
    Assert.assertEquals("Should have the correct physical type", Schema.Type.LONG,
        actual.getField("localDateTime").schema().getType());
    Assert.assertEquals("Should have the correct logical type", LogicalTypes.localTimestampMillis(),
        LogicalTypes.fromSchema(actual.getField("localDateTime").schema()));
  }

  private static <D> List<D> read(DatumReader<D> reader, File file) throws IOException {
    List<D> data = new ArrayList<>();

    try (FileReader<D> fileReader = new DataFileReader<>(file, reader)) {
      for (D datum : fileReader) {
        data.add(datum);
      }
    }

    return data;
  }

  private <D> File write(Schema schema, D... data) throws IOException {
    return write(ReflectData.get(), schema, data);
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
}

class RecordWithUUID {
  UUID uuid;

  @Override
  public int hashCode() {
    return uuid.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof RecordWithUUID)) {
      return false;
    }
    RecordWithUUID that = (RecordWithUUID) obj;
    return this.uuid.equals(that.uuid);
  }
}

class RecordWithStringUUID {
  String uuid;

  @Override
  public int hashCode() {
    return uuid.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof RecordWithStringUUID)) {
      return false;
    }
    RecordWithStringUUID that = (RecordWithStringUUID) obj;
    return this.uuid.equals(that.uuid);
  }
}

class RecordWithUUIDArray {
  UUID[] uuids;

  @Override
  public int hashCode() {
    return Arrays.hashCode(uuids);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof RecordWithUUIDArray)) {
      return false;
    }
    RecordWithUUIDArray that = (RecordWithUUIDArray) obj;
    return Arrays.equals(this.uuids, that.uuids);
  }
}

class RecordWithUUIDList {
  List<UUID> uuids;

  @Override
  public int hashCode() {
    return uuids.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof RecordWithUUIDList)) {
      return false;
    }
    RecordWithUUIDList that = (RecordWithUUIDList) obj;
    return this.uuids.equals(that.uuids);
  }
}

class RecordWithTimestamps {
  LocalDateTime localDateTime;

  @Override
  public int hashCode() {
    return Objects.hash(localDateTime);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof RecordWithTimestamps)) {
      return false;
    }
    RecordWithTimestamps that = (RecordWithTimestamps) obj;
    return Objects.equals(that.localDateTime, that.localDateTime);
  }
}
